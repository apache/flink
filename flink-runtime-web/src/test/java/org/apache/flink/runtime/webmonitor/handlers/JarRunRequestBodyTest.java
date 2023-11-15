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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.rest.messages.RestRequestMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JarRunRequestBody}. */
@ExtendWith(NoOpTestExtension.class)
class JarRunRequestBodyTest extends RestRequestMarshallingTestBase<JarRunRequestBody> {

    @Override
    protected Class<JarRunRequestBody> getTestRequestClass() {
        return JarRunRequestBody.class;
    }

    @Override
    protected JarRunRequestBody getTestRequestInstance() {
        return new JarRunRequestBody(
                "hello",
                "world",
                Arrays.asList("boo", "far"),
                4,
                new JobID(),
                true,
                "foo/bar",
                RestoreMode.CLAIM,
                Collections.singletonMap("key", "value"));
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            final JarRunRequestBody expected, final JarRunRequestBody actual) {
        assertThat(actual.getEntryClassName()).isEqualTo(expected.getEntryClassName());
        assertThat(actual.getProgramArguments()).isEqualTo(expected.getProgramArguments());
        assertThat(actual.getProgramArgumentsList()).isEqualTo(expected.getProgramArgumentsList());
        assertThat(actual.getParallelism()).isEqualTo(expected.getParallelism());
        assertThat(actual.getJobId()).isEqualTo(expected.getJobId());
        assertThat(actual.getAllowNonRestoredState())
                .isEqualTo(expected.getAllowNonRestoredState());
        assertThat(actual.getSavepointPath()).isEqualTo(expected.getSavepointPath());
        assertThat(actual.getRestoreMode()).isEqualTo(expected.getRestoreMode());
        assertThat(actual.getFlinkConfiguration().toMap())
                .containsExactlyEntriesOf(expected.getFlinkConfiguration().toMap());
    }
}
