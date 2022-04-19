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

package org.apache.flink.optimizer;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("serial")
public class BroadcastVariablePipelinebreakerTest extends CompilerTestBase {

    @Test
    void testNoBreakerForIndependentVariable() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<String> source1 = env.fromElements("test");
            DataSet<String> source2 = env.fromElements("test");

            source1.map(new IdentityMapper<String>())
                    .withBroadcastSet(source2, "some name")
                    .output(new DiscardingOutputFormat<String>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();

            assertThat(mapper.getInput().getTempMode()).isEqualTo(TempMode.NONE);
            assertThat(mapper.getBroadcastInputs().get(0).getTempMode()).isEqualTo(TempMode.NONE);

            assertThat(mapper.getInput().getDataExchangeMode())
                    .isEqualTo(DataExchangeMode.PIPELINED);
            assertThat(mapper.getBroadcastInputs().get(0).getDataExchangeMode())
                    .isEqualTo(DataExchangeMode.PIPELINED);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testBreakerForDependentVariable() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<String> source1 = env.fromElements("test");

            source1.map(new IdentityMapper<String>())
                    .map(new IdentityMapper<String>())
                    .withBroadcastSet(source1, "some name")
                    .output(new DiscardingOutputFormat<String>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode beforeMapper = (SingleInputPlanNode) mapper.getInput().getSource();

            assertThat(mapper.getInput().getTempMode()).isEqualTo(TempMode.NONE);
            assertThat(beforeMapper.getInput().getTempMode()).isEqualTo(TempMode.NONE);
            assertThat(mapper.getBroadcastInputs().get(0).getTempMode()).isEqualTo(TempMode.NONE);

            assertThat(mapper.getInput().getDataExchangeMode())
                    .isEqualTo(DataExchangeMode.PIPELINED);
            assertThat(beforeMapper.getInput().getDataExchangeMode())
                    .isEqualTo(DataExchangeMode.BATCH);
            assertThat(mapper.getBroadcastInputs().get(0).getDataExchangeMode())
                    .isEqualTo(DataExchangeMode.BATCH);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
