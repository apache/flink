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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TestStep;
import org.apache.flink.table.test.program.TestStep.TestKind;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/** Semantic tests for {@link StreamExecSink}. */
public class SinkSemanticTests extends SemanticTestBase {

    @Override
    public EnumSet<TestKind> supportedSetupSteps() {
        EnumSet<TestKind> steps = super.supportedSetupSteps();
        steps.add(TestKind.SINK_WITHOUT_DATA);
        return steps;
    }

    @Override
    protected void runStep(TestStep testStep, TableEnvironment env) throws Exception {
        if (testStep.getKind() == TestKind.SINK_WITHOUT_DATA) {
            final SinkTestStep sinkTestStep = (SinkTestStep) testStep;
            sinkTestStep.apply(
                    env,
                    Map.ofEntries(
                            Map.entry("connector", "values"),
                            Map.entry("sink-insert-only", "false")));
        } else {
            super.runStep(testStep, env);
        }
    }

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                SinkTestPrograms.INSERT_RETRACT_WITHOUT_PK,
                SinkTestPrograms.INSERT_RETRACT_WITH_PK,
                SinkTestPrograms.ON_CONFLICT_DO_NOTHING_KEEPS_FIRST,
                SinkTestPrograms.ON_CONFLICT_DO_ERROR_NO_CONFLICT,
                SinkTestPrograms.UPSERT_KEY_DIFFERS_FROM_PK_WITHOUT_ON_CONFLICT,
                SinkTestPrograms.UPSERT_KEY_DIFFERS_FROM_PK_WITH_ON_CONFLICT,
                SinkTestPrograms.UPSERT_KEY_MATCHES_PK_WITHOUT_ON_CONFLICT,
                SinkTestPrograms.APPEND_ONLY_WITH_PK_WITHOUT_ON_CONFLICT,
                SinkTestPrograms.APPEND_ONLY_WITH_PK_WITH_ON_CONFLICT,
                SinkTestPrograms.UPSERT_KEY_DIFFERS_FROM_PK_WITHOUT_ON_CONFLICT_DISABLED,
                SinkTestPrograms.ON_CONFLICT_NOT_ALLOWED_FOR_APPEND_ONLY_SINK,
                SinkTestPrograms.ON_CONFLICT_NOT_ALLOWED_FOR_RETRACT_SINK);
    }
}
