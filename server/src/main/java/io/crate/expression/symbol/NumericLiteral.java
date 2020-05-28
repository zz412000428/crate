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

package io.crate.expression.symbol;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.NumericType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;

import java.math.BigDecimal;

public class NumericLiteral extends Literal<Number> {

    public NumericLiteral(Number value) {
        this(value, false);
    }

    public NumericLiteral(Number value, boolean floatingPoint) {
        super(floatingPoint ? new NumericType(DataType.Precedence.DOUBLE) : NumericType.INSTANCE, value);
    }

    @Override
    public Symbol cast(DataType<?> targetType, boolean tryCast, boolean explicitCast) {
        switch (targetType.id()) {
            case ShortType.ID:
                return Literal.of(DataTypes.SHORT, value().shortValue());
            case IntegerType.ID:
                return Literal.of(value().intValue());
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return Literal.of(value().longValue());
            case FloatType.ID:
                return Literal.of(value().floatValue());
            case DoubleType.ID:
                return Literal.of(value().doubleValue());
            default:
                return super.cast(targetType, tryCast, explicitCast);
        }
    }

    public Literal<Number> negate() {
        var val = value();
        if (val instanceof Long) {
            return new NumericLiteral(-1 * val.longValue());
        } else if (val instanceof Integer) {
            return new NumericLiteral(-1 * val.intValue());
        } else if (val instanceof Short) {
            return new NumericLiteral(-1 * val.shortValue());
        } else if (val instanceof Float) {
            return new NumericLiteral(-1 * val.floatValue());
        } else if (val instanceof Double) {
            return new NumericLiteral(-1 * val.doubleValue());
        } else if (val instanceof BigDecimal) {
            return new NumericLiteral(((BigDecimal) val).negate());
        }
        throw new UnsupportedOperationException(Symbols.format(
            "Cannot negate %s. You may need to add explicit type casts", this));
    }
}
