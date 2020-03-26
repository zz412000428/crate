/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static io.crate.expression.scalar.SubscriptObjectFunction.tryToInferReturnTypeFromObjectTypeAndArguments;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

/** Supported subscript expressions:
 * <ul>
 *     <li>obj['x']</li>
 *     <li>arr[1]</li>
 *     <li>obj_array[1]</li>
 *     <li>obj_array['x']</li>
 * </ul>
 **/
public class SubscriptFunction extends Scalar<Object, Object[]> {

    public static final String NAME = "subscript";

    private final FunctionInfo info;
    private final BiFunction<Object, Object, Object> lookup;

    public static void register(ScalarFunctionModule module) {
        // subscript(array(E), numeric) -> element
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("integer"),
                parseTypeSignature("E")
            )
            .withTypeVariableConstraints(
                typeVariable("E")
                    // `object` must be excluded to make it more specific for anything than `object`.
                    // Otherwise the signature of `subscript(array(object()), key)` could also match on coercion.
                    .withExcludedTypes(parseTypeSignature("object"))
            ),
            argumentTypes ->
                new SubscriptFunction(
                    new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.UNDEFINED),
                    SubscriptFunction::lookupByNumericIndex
                )
        );

        // subscript(array(object()), key) -> element[]
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(object)"),
                parseTypeSignature("text"),
                parseTypeSignature("array(undefined)")
            ),
            argumentTypes ->
                new SubscriptFunction(
                    new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.UNDEFINED),
                    SubscriptFunction::lookupIntoListObjectsByName
                )
        );

        // subscript(object(text, element), text) -> element
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("object"),
                parseTypeSignature("text"),
                parseTypeSignature("undefined")
            ),
            argumentTypes ->
                new SubscriptFunction(
                    new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.UNDEFINED),
                    SubscriptFunction::lookupByName
                )
        );
    }

    private SubscriptFunction(FunctionInfo info, BiFunction<Object, Object, Object> lookup) {
        this.info = info;
        this.lookup = lookup;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function func, TransactionContext txnCtx) {
        Symbol result = evaluateIfLiterals(this, txnCtx, func);
        if (result instanceof Literal) {
            return result;
        }
        if (func.arguments().get(0).valueType().id() == ObjectType.ID) {
            return tryToInferReturnTypeFromObjectTypeAndArguments(func);
        }
        return func;
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length == 2 : "invalid number of arguments";
        Object element = args[0].value();
        Object index = args[1].value();
        if (element == null || index == null) {
            return null;
        }
        return lookup.apply(element, index);
    }

    static Object lookupIntoListObjectsByName(Object base, Object name) {
        List<?> values = (List<?>) base;
        List<Object> result = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            Map<?, ?> map = (Map<?, ?>) values.get(i);
            result.add(map.get(name));
        }
        return result;
    }

    static Object lookupByNumericIndex(Object base, Object index) {
        List<?> values = (List<?>) base;
        int idx = (Integer) index;
        try {
            return values.get(idx - 1); // SQL array indices start with 1
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    static Object lookupByName(Object base, Object name) {
        assert base instanceof Map : "Base argument to subscript must be a Map, not " + base;
        Map<?, ?> map = (Map<?, ?>) base;
        if (!map.containsKey(name)) {
            throw new IllegalArgumentException("The object `" + base + "` does not contain the key `" + name + "`");
        }
        return map.get(name);
    }
}
