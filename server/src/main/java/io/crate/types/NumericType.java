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

package io.crate.types;

import io.crate.Streamer;

public class NumericType extends DataType<Number> {

    public static final int ID = 16;
    public static final String NAME = "numeric";
    public static final NumericType INSTANCE = new NumericType();

    private final Precedence precedence;

    public NumericType() {
        this(Precedence.NUMERIC);
    }

    public NumericType(Precedence precedence) {
        this.precedence = precedence;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return precedence;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<Number> streamer() {
        throw new UnsupportedOperationException("Streaming is not supported for 'numeric' meta type");
    }

    @Override
    public Number value(Object value) throws IllegalArgumentException, ClassCastException {
        return (Number) value;
    }

    @Override
    public int compare(Number o1, Number o2) {
        if (o1 instanceof Double || o2 instanceof Double) {
            return Double.compare(o1.doubleValue(), o2.doubleValue());
        }
        return Long.compare(o1.longValue(), o2.longValue());
    }


}
