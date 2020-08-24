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

package io.crate.planner.optimizer;

import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;
import org.elasticsearch.Version;

public interface Rule<T> {

    Pattern<T> pattern();

    LogicalPlan apply(T plan, Captures captures, TableStats tableStats, TransactionContext txnCtx, Functions functions);

    /**
     * @return The version all nodes in the cluster must have to be able to use this optimization.
     */
    default Version requiredVersion() {
        return Version.V_4_0_0;
    }
}
