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

package org.apache.flink.table.runtime.runners.python.beam;

import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.Constants;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.junit.jupiter.api.Test;

import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createRowTypeCoderInfoDescriptorProto;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BeamProcessTableFunctionRunner}. */
class BeamProcessTableFunctionRunnerTest {

    private static final RowType DATA_TYPE = RowType.of(new BigIntType());

    @Test
    void testBuildsProcessTableFunctionTransformWithoutTimers() throws Exception {
        final BeamProcessTableFunctionRunner runner = createRunner(false);
        final RunnerApi.ParDoPayload payload = buildPayload(runner);

        assertThat(payload.getDoFn().getUrn())
                .isEqualTo(BeamProcessTableFunctionRunner.PROCESS_TABLE_FUNCTION_URN);
        assertThat(payload.getTimerFamilySpecsMap()).isEmpty();
        assertThat(runner.getOptionalTimerCoderProto()).isEmpty();
    }

    @Test
    void testDeclaresEventTimeTimerFamily() throws Exception {
        final BeamProcessTableFunctionRunner runner = createRunner(true);
        final RunnerApi.ParDoPayload payload = buildPayload(runner);

        assertThat(payload.getTimerFamilySpecsMap()).containsOnlyKeys(Constants.TIMER_ID);
        assertThat(payload.getTimerFamilySpecsOrThrow(Constants.TIMER_ID).getTimeDomain())
                .isEqualTo(RunnerApi.TimeDomain.Enum.EVENT_TIME);
        assertThat(runner.getOptionalTimerCoderProto()).isPresent();
    }

    private static RunnerApi.ParDoPayload buildPayload(BeamProcessTableFunctionRunner runner)
            throws Exception {
        final RunnerApi.Components.Builder components = RunnerApi.Components.newBuilder();
        runner.buildTransforms(components);
        return RunnerApi.ParDoPayload.parseFrom(
                components.getTransformsOrThrow(Constants.TRANSFORM_ID).getSpec().getPayload());
    }

    private static BeamProcessTableFunctionRunner createRunner(boolean withTimers) {
        final FlinkFnApi.CoderInfoDescriptor dataCoder =
                createFlattenRowTypeCoderInfoDescriptorProto(
                        DATA_TYPE, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
        return new BeamProcessTableFunctionRunner(
                null,
                "test-task",
                PythonTestUtils.createTestProcessEnvironmentManager(),
                FlinkFnApi.UserDefinedProcessTableFunction.getDefaultInstance(),
                null,
                null,
                null,
                null,
                null,
                0.0,
                dataCoder,
                dataCoder,
                withTimers
                        ? createRowTypeCoderInfoDescriptorProto(
                                DATA_TYPE, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false)
                        : null);
    }
}
