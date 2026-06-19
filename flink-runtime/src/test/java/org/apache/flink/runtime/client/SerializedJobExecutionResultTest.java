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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the SerializedJobExecutionResult */
class SerializedJobExecutionResultTest {

    @Test
    void testSerialization() throws Exception {
        final ClassLoader classloader = getClass().getClassLoader();

        JobID origJobId = new JobID();
        long origTime = 65927436589267L;

        Map<String, SerializedValue<OptionalFailure<Object>>> origMap = new HashMap<>();
        origMap.put("name1", new SerializedValue<>(OptionalFailure.of(723L)));
        origMap.put("name2", new SerializedValue<>(OptionalFailure.of("peter")));
        origMap.put(
                "name3",
                new SerializedValue<>(OptionalFailure.ofFailure(new ExpectedTestException())));

        SerializedJobExecutionResult result =
                new SerializedJobExecutionResult(origJobId, origTime, origMap);

        // serialize and deserialize the object
        SerializedJobExecutionResult cloned = CommonTestUtils.createCopySerializable(result);

        assertThat(cloned.getJobId()).isEqualTo(origJobId);
        assertThat(cloned.getNetRuntime()).isEqualTo(origTime);
        assertThat(cloned.getNetRuntime(TimeUnit.MILLISECONDS)).isEqualTo(origTime);
        assertThat(cloned.getSerializedAccumulatorResults()).isEqualTo(origMap);

        // convert to deserialized result
        JobExecutionResult jResult = result.toJobExecutionResult(classloader);
        JobExecutionResult jResultCopied = result.toJobExecutionResult(classloader);

        assertThat(jResult.getJobID()).isEqualTo(origJobId);
        assertThat(jResultCopied.getJobID()).isEqualTo(origJobId);
        assertThat(jResult.getNetRuntime()).isEqualTo(origTime);
        assertThat(jResult.getNetRuntime(TimeUnit.MILLISECONDS)).isEqualTo(origTime);
        assertThat(jResultCopied.getNetRuntime()).isEqualTo(origTime);
        assertThat(jResultCopied.getNetRuntime(TimeUnit.MILLISECONDS)).isEqualTo(origTime);

        for (Map.Entry<String, SerializedValue<OptionalFailure<Object>>> entry :
                origMap.entrySet()) {
            String name = entry.getKey();
            OptionalFailure<Object> value = entry.getValue().deserializeValue(classloader);
            if (value.isFailure()) {
                assertThatThrownBy(() -> jResult.getAccumulatorResult(name))
                        .isInstanceOf(FlinkRuntimeException.class)
                        .hasCauseInstanceOf(ExpectedTestException.class);

                assertThatThrownBy(() -> jResultCopied.getAccumulatorResult(name))
                        .isInstanceOf(FlinkRuntimeException.class)
                        .hasCauseInstanceOf(ExpectedTestException.class);
            } else {
                assertThat((Object) jResult.getAccumulatorResult(name)).isEqualTo(value.get());
                assertThat((Object) jResultCopied.getAccumulatorResult(name))
                        .isEqualTo(value.get());
            }
        }
    }

    @Test
    void testSerializationWithNullValues() throws Exception {
        SerializedJobExecutionResult result = new SerializedJobExecutionResult(null, 0L, null);
        SerializedJobExecutionResult cloned = CommonTestUtils.createCopySerializable(result);

        assertThat(cloned.getJobId()).isNull();
        assertThat(cloned.getNetRuntime()).isZero();
        assertThat(cloned.getSerializedAccumulatorResults()).isNull();

        JobExecutionResult jResult = result.toJobExecutionResult(getClass().getClassLoader());
        assertThat(jResult.getJobID()).isNull();
        assertThat(jResult.getAllAccumulatorResults()).isEmpty();
    }
}
