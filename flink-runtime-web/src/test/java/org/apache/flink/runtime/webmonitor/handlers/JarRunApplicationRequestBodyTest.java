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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.rest.messages.RestRequestMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JarRunApplicationRequestBody}. */
@ExtendWith(NoOpTestExtension.class)
class JarRunApplicationRequestBodyTest
        extends RestRequestMarshallingTestBase<JarRunApplicationRequestBody> {

    @Override
    protected Class<JarRunApplicationRequestBody> getTestRequestClass() {
        return JarRunApplicationRequestBody.class;
    }

    @Override
    protected JarRunApplicationRequestBody getTestRequestInstance() {
        return new JarRunApplicationRequestBody(
                "hello",
                Arrays.asList("boo", "far"),
                4,
                new JobID(),
                true,
                "foo/bar",
                RecoveryClaimMode.CLAIM,
                Collections.singletonMap("key", "value"),
                new ApplicationID());
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            final JarRunApplicationRequestBody expected,
            final JarRunApplicationRequestBody actual) {
        assertThat(actual.getEntryClassName()).isEqualTo(expected.getEntryClassName());
        assertThat(actual.getProgramArgumentsList()).isEqualTo(expected.getProgramArgumentsList());
        assertThat(actual.getParallelism()).isEqualTo(expected.getParallelism());
        assertThat(actual.getJobId()).isEqualTo(expected.getJobId());
        assertThat(actual.getAllowNonRestoredState())
                .isEqualTo(expected.getAllowNonRestoredState());
        assertThat(actual.getSavepointPath()).isEqualTo(expected.getSavepointPath());
        assertThat(actual.getRecoveryClaimMode()).isEqualTo(expected.getRecoveryClaimMode());
        assertThat(actual.getFlinkConfiguration().toMap())
                .containsExactlyEntriesOf(expected.getFlinkConfiguration().toMap());
    }
}
