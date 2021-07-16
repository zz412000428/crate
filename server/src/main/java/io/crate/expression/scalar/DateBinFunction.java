/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import org.elasticsearch.common.TriFunction;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public class DateBinFunction extends Scalar<Long, Object> {

    public static final String NAME = "date_bin";

    private final Function<Object, Long> intervalToLongOperator = interval -> Long.valueOf(((Period) interval).toStandardDuration().getMillis());

    // First argument is a result of intervalOperator
    private final TriFunction<Long, Object, Object, Long> dateBinOperator;

    // Interval is same for all rows, computed once in compile()
    // in order to avoid toStandardDuration() call and object allocation per row.
    @Nullable
    private Long compiledInterval;

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature(), // source
                DataTypes.TIMESTAMPZ.getTypeSignature(), // origin
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            DateBinFunction::new);

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.TIMESTAMP.getTypeSignature(), // source
                DataTypes.TIMESTAMP.getTypeSignature(), // origin
                DataTypes.TIMESTAMP.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            DateBinFunction::new);
    }

    private final Signature signature;
    private final Signature boundSignature;

    DateBinFunction(Signature signature, Signature boundSignature) {
        this(signature, boundSignature, null);
    }

    private DateBinFunction(Signature signature, Signature boundSignature, @Nullable Long compiledInterval) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.compiledInterval = compiledInterval;

        if (signature.getArgumentDataTypes().get(1).id() == DataTypes.TIMESTAMP.id()) {
            dateBinOperator = (interval, timestamp, origin) ->
                getBinnedTimestamp(interval, DataTypes.TIMESTAMP.implicitCast(timestamp), DataTypes.TIMESTAMP.implicitCast(origin));
        } else if (signature.getArgumentDataTypes().get(1).id() == DataTypes.TIMESTAMPZ.id()) {
            dateBinOperator = (interval, timestamp, origin) ->
               getBinnedTimestamp(interval, DataTypes.TIMESTAMPZ.implicitCast(timestamp), DataTypes.TIMESTAMPZ.implicitCast(origin));
        } else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "%s is not supported as timestamp argument type", signature.getArgumentDataTypes().get(1).getName()));
        }
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Scalar<Long, Object> compile(List<Symbol> arguments) {
        assert arguments.size() == 3 : "Invalid number of arguments";

        if (!arguments.get(0).symbolType().isValueSymbol()) {
            // interval is not a value, we can't compile
            return this;
        }

        Long interval = intervalToLongOperator.apply(((Input<?>) arguments.get(0)).value());
        return new DateBinFunction(signature, boundSignature, interval);
    }

    @Override
    public final Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 3 : "Invalid number of arguments";

        var interval = args[0].value();
        var timestamp = args[1].value();
        var origin = args[2].value();

        if (interval == null || timestamp == null || origin == null) {
            return null;
        }

        if (compiledInterval != null) {
            return dateBinOperator.apply(compiledInterval, timestamp, origin);
        } else {
            // If all arguments are literals function gets normalized to literal (for example test run with some literal value for timestamp)
            // and compile is never called (and it's not needed as normalized version is already fast)
            return dateBinOperator.apply(intervalToLongOperator.apply(interval), timestamp, origin);
        }
    }

    private static long getBinnedTimestamp(long interval, long timestamp, long origin) {
        if (interval == 0) {
            throw new IllegalArgumentException("Interval cannot be zero");
        }

        /*
         in Java % operator returns negative result only if dividend is negative.
         https://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.17.3

         We need to shift timestamp by the timeline to the "earlier" direction, to the left.
         If diff is negative, remainder will be also negative (see link above), we subtract negative
         to move to right side of the bin and then subtract abs(interval) to move to beginning of the bin.
        */

        long diff = timestamp - origin;
        if (diff >= 0) {
            // diff % interval >= 0 regardless of the interval sign.
            return timestamp - diff % interval;
        } else {
            // diff % interval < 0 regardless of the interval sign.
            if (interval > 0) {
                return timestamp - diff % interval - interval;
            } else {
                return timestamp - diff % interval + interval;
            }
        }

    }
}
