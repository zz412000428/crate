/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.functions;

import io.crate.types.TypeSignature;

import java.util.List;
import java.util.Objects;

public class TypeVariableConstraint {

    public static TypeVariableConstraint typeVariable(String name) {
        return new TypeVariableConstraint(name, List.of(), false);
    }

    public static TypeVariableConstraint typeVariableOfAnyType(String name) {
        return new TypeVariableConstraint(name, List.of(), true);
    }

    private final String name;
    private final List<TypeSignature> excludedTypes;
    private final boolean anyAllowed;

    public TypeVariableConstraint(String name, List<TypeSignature> excludedTypes, boolean anyAllowed) {
        this.name = name;
        this.excludedTypes = excludedTypes;
        this.anyAllowed = anyAllowed;
    }

    public String getName() {
        return name;
    }

    public List<TypeSignature> getExcludedTypes() {
        return excludedTypes;
    }

    public boolean isAnyAllowed() {
        return anyAllowed;
    }

    public TypeVariableConstraint withExcludedTypes(TypeSignature... excludedTypes) {
        return new TypeVariableConstraint(name, List.of(excludedTypes), anyAllowed);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeVariableConstraint that = (TypeVariableConstraint) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
