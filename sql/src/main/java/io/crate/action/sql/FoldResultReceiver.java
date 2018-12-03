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

package io.crate.action.sql;

import io.crate.data.Row;
import io.crate.protocols.postgres.ClientInterrupted;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public final class FoldResultReceiver<S> implements ResultReceiver<S> {

    private final CompletableFuture<S> result = new CompletableFuture<>();
    private final BiFunction<Row, S, S> onRow;
    private S state;

    public static <S> FoldResultReceiver<S> forMutableState(BiConsumer<Row, S> onRow, S initialState) {
        BiFunction<Row, S, S> processRow = (row, state) -> {
            onRow.accept(row, state);
            return state;
        };
        return new FoldResultReceiver<>(processRow, initialState);
    }

    public FoldResultReceiver(BiFunction<Row, S, S> onRow, S initialState) {
        this.onRow = onRow;
        this.state = initialState;
    }

    @Override
    public void setNextRow(Row row) {
        state = onRow.apply(row, state);
    }

    @Override
    public void batchFinished() {
    }

    @Override
    public void allFinished(boolean interrupted) {
        if (interrupted) {
            result.completeExceptionally(new ClientInterrupted());
        } else {
            result.complete(state);
        }
    }

    @Override
    public void fail(@Nonnull Throwable t) {
        result.completeExceptionally(t);
    }

    @Override
    public CompletableFuture<S> completionFuture() {
        return result;
    }
}
