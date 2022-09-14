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

package org.apache.flink.api.java;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.java.io.DiscardingOutputFormat;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for multiple invocations of a plan. */
class MultipleInvokationsTest {

    @Test
    void testMultipleInvocationsGetPlan() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // ----------- Execution 1 ---------------

            DataSet<String> data = env.fromElements("Some", "test", "data").name("source1");
            // data.print();
            data.output(new DiscardingOutputFormat<>()).name("print1");
            data.output(new DiscardingOutputFormat<>()).name("output1");

            {
                Plan p = env.createProgramPlan();

                assertThat(p.getDataSinks()).hasSize(2);
                for (GenericDataSinkBase<?> sink : p.getDataSinks()) {
                    assertThat(sink.getName()).isIn("print1", "output1");
                    assertThat(sink.getInput().getName()).isEqualTo("source1");
                }
            }

            // ----------- Execution 2 ---------------

            data.writeAsText("/some/file/path").name("textsink");

            {
                Plan p = env.createProgramPlan();

                assertThat(p.getDataSinks()).hasSize(1);
                GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
                assertThat(sink.getName()).isEqualTo("textsink");
                assertThat(sink.getInput().getName()).isEqualTo("source1");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
