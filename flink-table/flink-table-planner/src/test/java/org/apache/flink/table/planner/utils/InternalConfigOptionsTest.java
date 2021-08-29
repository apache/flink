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

package org.apache.flink.table.planner.utils;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala;

/** Tests for {@link InternalConfigOptions}. */
@RunWith(Parameterized.class)
public class InternalConfigOptionsTest extends TableTestBase {

    private TableEnvironment tEnv;
    private PlannerBase planner;

    @Parameterized.Parameters(name = "plannerMode = {0}")
    public static Collection<String> parameters() {
        return Arrays.asList("STREAMING", "BATCH");
    }

    @Parameterized.Parameter public String plannerMode;

    @Before
    public void setUp() {
        if (plannerMode.equals("STREAMING")) {
            StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());
            tEnv = util.getTableEnv();
            planner = util.getPlanner();
        } else {
            BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());
            tEnv = util.getTableEnv();
            planner = util.getPlanner();
        }
    }

    @Test
    public void testTranslateExecNodeGraphWithInternalTemporalConf() {
        Table table =
                tEnv.sqlQuery("SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_TIME, CURRENT_TIMESTAMP");
        RelNode relNode = planner.optimize(TableTestUtil.toRelNode(table));
        ExecNodeGraph execNodeGraph =
                planner.translateToExecNodeGraph(toScala(Collections.singletonList(relNode)));
        // PlannerBase#translateToExecNodeGraph will set internal temporal configurations and
        // cleanup them after translate finished
        List<Transformation<?>> transformation = planner.translateToPlan(execNodeGraph);
        // check the translation success
        Assert.assertEquals(1, transformation.size());
    }
}
