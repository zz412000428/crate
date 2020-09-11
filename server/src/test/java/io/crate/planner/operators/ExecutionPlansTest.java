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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.table.Operation;
import io.crate.planner.DependencyCarrier;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.TestingRowConsumer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ExecutionPlansTest extends SQLTransportIntegrationTest {

    private static final List<BiFunction<LogicalPlan, Function<String, Symbol>, LogicalPlan>> TEST_PLANS = new ArrayList<>();

    private static final String PLAN_SOURCE_STMT = "select x + 1, x from doc.t1";
    private static final String PLAN_SOURCE_STMT_WITH_PARAMS = "select x + ?, x from doc.t1";

    static {
        TEST_PLANS.add(
            (source, builder) -> new Filter(source, builder.apply("x > ?"))
        );
        TEST_PLANS.add(
            (source, builder) -> new Limit(source, builder.apply("?"), builder.apply("?"))
        );
        TEST_PLANS.add(
            (source, builder) -> new Order(source, new OrderBy(List.of(builder.apply("?"))))
        );
        TEST_PLANS.add(
            (source, builder) -> new HashAggregate(
                source,
                List.of((io.crate.expression.symbol.Function) builder.apply("sum(x + ?)"))
            )
        );
    }

    private String nodeName;
    private DependencyCarrier dependencyCarrier;
    private RelationAnalyzer relationAnalyzer;

    private static final Row PARAMS = new Row() {
        @Override
        public int numColumns() {
            return -1;
        }

        @Override
        public Object get(int index) {
            return 1;
        }
    };

    @Before
    public void prepare() throws Exception {
        execute("create table doc.t1 (x int)");
        String[] nodeNames = internalCluster().getNodeNames();
        nodeName = nodeNames[randomIntBetween(1, nodeNames.length) - 1];
        dependencyCarrier = internalCluster().getInstance(DependencyCarrier.class, nodeName);
        relationAnalyzer = internalCluster().getInstance(RelationAnalyzer.class, nodeName);
    }

    private ExpressionAnalyzer createExpressionAnalyzer(PlanForNode planForNode) {
        var plannerContext = planForNode.plannerContext;
        return new ExpressionAnalyzer(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            ParamTypeHints.EMPTY,
            new FullQualifiedNameFieldProvider(
                Map.of(((LogicalPlan) planForNode.plan).getRelationNames().iterator().next(),
                       ((LogicalPlan) planForNode.plan).baseTables().get(0)),
                ParentRelations.NO_PARENTS,
                plannerContext.transactionContext().sessionContext().searchPath().currentSchema()),
            new SubqueryAnalyzer(
                relationAnalyzer,
                new StatementAnalysisContext(ParamTypeHints.EMPTY, Operation.READ, plannerContext.transactionContext())
            )
        );
    }

    @Test
    public void test_each_logical_plan_results_in_valid_execution_plan() throws Exception {
        var planForNode = plan(PLAN_SOURCE_STMT, nodeName);
        var source = ((LogicalPlan) planForNode.plan).sources().get(0);
        var expressionAnalyzer = createExpressionAnalyzer(planForNode);

        for (var testPlan : TEST_PLANS) {

            // No parameters (they are replaced upfront)
            var paramBinder = new SubQueryAndParamBinder(PARAMS, SubQueryResults.EMPTY);
            var logicalPlan = testPlan.apply(
                source,
                s -> paramBinder.apply(
                    expressionAnalyzer.convert(SqlParser.createExpression(s), new ExpressionAnalysisContext())
                )
            );
            var consumer = new TestingRowConsumer();
            logicalPlan.executeOrFail(
                dependencyCarrier,
                planForNode.plannerContext,
                consumer,
                Row.EMPTY,
                SubQueryResults.EMPTY
            );
            // this must not throw any exception
            consumer.completionFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void test_each_logical_plan_results_in_valid_execution_plan_with_params_used() throws Exception {
        var planForNode = plan(PLAN_SOURCE_STMT_WITH_PARAMS, nodeName);
        var source = ((LogicalPlan) planForNode.plan).sources().get(0);
        var expressionAnalyzer = createExpressionAnalyzer(planForNode);

        for (var testPlan : TEST_PLANS) {

            // Using parameter symbols, binding must happen correctly inside operators
            var logicalPlan = testPlan.apply(
                source,
                s -> expressionAnalyzer.convert(SqlParser.createExpression(s), new ExpressionAnalysisContext())
            );
            var consumer = new TestingRowConsumer();
            logicalPlan.executeOrFail(
                dependencyCarrier,
                planForNode.plannerContext,
                consumer,
                PARAMS,
                SubQueryResults.EMPTY
            );
            // this must not throw any exception
            consumer.completionFuture().get(5, TimeUnit.SECONDS);
        }
    }
}
