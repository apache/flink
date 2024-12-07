/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitorImpl;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for loading plan to exec node graph. */
@ExtendWith(ParameterizedTestExtension.class)
class LoadJsonPlanTest extends JsonPlanTestBase {

    private final TableConfigOptions.CatalogPlanCompilation compileCatalogObjectsLevel;

    @Parameters(name = "compileCatalogObjectsLevel = {0}")
    private static Collection<TableConfigOptions.CatalogPlanCompilation> data() {
        return Arrays.asList(TableConfigOptions.CatalogPlanCompilation.values());
    }

    LoadJsonPlanTest(TableConfigOptions.CatalogPlanCompilation compileCatalogObjectsLevel) {
        this.compileCatalogObjectsLevel = compileCatalogObjectsLevel;
    }

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();

        tableEnv.getConfig()
                .set(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS, compileCatalogObjectsLevel);

        tableEnv.executeSql(
                "CREATE TABLE src (\n"
                        + "  a int,\n"
                        + "  b varchar,\n"
                        + "  c int,\n"
                        + "  proctime as PROCTIME()"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false'\n"
                        + ")");
        tableEnv.executeSql(
                "CREATE TABLE dim (\n"
                        + "  a int,\n"
                        + "  b varchar,\n"
                        + "  c int\n"
                        + ") with (\n"
                        + "  'connector' = 'values'\n"
                        + ")");

        tableEnv.executeSql(
                "CREATE TABLE snk (\n"
                        + "  a int,\n"
                        + "  b varchar,\n"
                        + "  c int\n"
                        + ") with (\n"
                        + "  'connector' = 'values'\n"
                        + ")");
    }

    @TestTemplate
    void testLoadPlanWithHintsOnLookupSource() {
        String sql =
                "insert into snk select src.a, src.b, src.c from src "
                        + "join dim /*+ OPTIONS('async'='true') */ for system_time as of "
                        + "src.proctime on src.a = dim.a";
        ExecNodeGraph execNodeGraph = compileSqlAndLoadPlan(sql);
        StreamExecLookupJoin lookupJoin =
                getSingleSpecificExecNodes(execNodeGraph, StreamExecLookupJoin.class);

        DynamicTableSourceSpec spec = lookupJoin.getTemporalTableSourceSpec().getTableSourceSpec();
        assertThat(spec).isNotNull();
        Map<String, String> expectedExtraOptions = Collections.singletonMap("async", "true");

        // verify the dynamic options in spec
        assertThat(spec.getDynamicOptions()).isNotNull().isEqualTo(expectedExtraOptions);

        // verify the table options in dim table
        Map<String, String> tableOptions = spec.getContextResolvedTable().getTable().getOptions();
        assertThat(tableOptions).containsAllEntriesOf(expectedExtraOptions);
    }

    @TestTemplate
    void testLoadPlanWithHintsOnScanSource() {
        String sql =
                "insert into snk select src.a, src.b, src.c from src /*+ OPTIONS('source.sleep-time'='10s') */ ";

        ExecNodeGraph execNodeGraph = compileSqlAndLoadPlan(sql);
        StreamExecTableSourceScan source =
                getSingleSpecificExecNodes(execNodeGraph, StreamExecTableSourceScan.class);

        DynamicTableSourceSpec spec = source.getTableSourceSpec();
        Map<String, String> expectedExtraOptions =
                Collections.singletonMap("source.sleep-time", "10s");

        // verify the dynamic options in spec
        assertThat(spec.getDynamicOptions()).isNotNull().isEqualTo(expectedExtraOptions);

        // verify the table options in table
        Map<String, String> tableOptions = spec.getContextResolvedTable().getTable().getOptions();
        assertThat(tableOptions).containsAllEntriesOf(expectedExtraOptions);
    }

    @TestTemplate
    void testLoadPlanWithHintsOnSink() {
        String sql =
                "insert into snk /*+ OPTIONS('sink-insert-only'='false') */ select src.a, src.b, src.c from src";

        ExecNodeGraph execNodeGraph = compileSqlAndLoadPlan(sql);
        StreamExecSink sink = getSingleSpecificExecNodes(execNodeGraph, StreamExecSink.class);

        DynamicTableSinkSpec spec = sink.getTableSinkSpec();
        Map<String, String> expectedExtraOptions =
                Collections.singletonMap("sink-insert-only", "false");

        // verify the dynamic options in spec
        assertThat(spec.getDynamicOptions()).isNotNull().isEqualTo(expectedExtraOptions);

        // verify the table options in table
        Map<String, String> tableOptions = spec.getContextResolvedTable().getTable().getOptions();
        assertThat(tableOptions).containsAllEntriesOf(expectedExtraOptions);
    }

    private <T extends ExecNode<?>> T getSingleSpecificExecNodes(
            ExecNodeGraph execNodeGraph, Class<T> expectExecNodeClazz) {
        Set<T> collectResults = getSpecificExecNodes(execNodeGraph, expectExecNodeClazz);
        if (collectResults.size() > 1) {
            throw new IllegalArgumentException(
                    "There are more than one "
                            + expectExecNodeClazz.getSimpleName()
                            + " in the execNodeGraph");
        }
        if (collectResults.isEmpty()) {
            throw new IllegalArgumentException(
                    "There is no " + expectExecNodeClazz.getSimpleName() + " in the execNodeGraph");
        }
        return collectResults.iterator().next();
    }

    private <T extends ExecNode<?>> Set<T> getSpecificExecNodes(
            ExecNodeGraph execNodeGraph, Class<T> clazz) {
        final Set<T> collectResults = Sets.newIdentityHashSet();

        final ExecNodeVisitor collector =
                new ExecNodeVisitorImpl() {
                    @Override
                    public void visit(ExecNode<?> node) {
                        if (clazz.isInstance(node)) {
                            collectResults.add((T) node);
                        }
                        super.visit(node);
                    }
                };

        execNodeGraph.getRootNodes().forEach(collector::visit);
        return collectResults;
    }
}
