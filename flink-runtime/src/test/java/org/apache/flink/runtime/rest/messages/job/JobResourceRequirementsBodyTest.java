/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.RestRequestMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for the marshalling of {@link JobResourceRequirementsBody}. */
@ExtendWith(NoOpTestExtension.class)
public class JobResourceRequirementsBodyTest
        extends RestRequestMarshallingTestBase<JobResourceRequirementsBody> {
    @Override
    protected Class<JobResourceRequirementsBody> getTestRequestClass() {
        return JobResourceRequirementsBody.class;
    }

    @Override
    protected JobResourceRequirementsBody getTestRequestInstance() {
        return new JobResourceRequirementsBody(
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(new JobVertexID(), 1, 42)
                        .setParallelismForJobVertex(new JobVertexID(), 1, 1337)
                        .build());
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            JobResourceRequirementsBody expected, JobResourceRequirementsBody actual) {
        assertThat(expected).satisfies(matching(equalsChangeJobRequestBody(actual)));
    }

    private EqualityChangeJobRequestBodyMatcher equalsChangeJobRequestBody(
            JobResourceRequirementsBody actual) {
        return new EqualityChangeJobRequestBodyMatcher(actual);
    }

    private static final class EqualityChangeJobRequestBodyMatcher
            extends TypeSafeMatcher<JobResourceRequirementsBody> {

        private final JobResourceRequirementsBody actualJobResourceRequirementsBody;

        private EqualityChangeJobRequestBodyMatcher(
                JobResourceRequirementsBody actualJobResourceRequirementsBody) {
            this.actualJobResourceRequirementsBody = actualJobResourceRequirementsBody;
        }

        @Override
        protected boolean matchesSafely(JobResourceRequirementsBody jobResourceRequirementsBody) {
            final Optional<JobResourceRequirements> maybeActualJobResourceRequirements =
                    actualJobResourceRequirementsBody.asJobResourceRequirements();
            final Optional<JobResourceRequirements> maybeJobResourceRequirements =
                    jobResourceRequirementsBody.asJobResourceRequirements();
            if (maybeActualJobResourceRequirements.isPresent()
                    ^ maybeJobResourceRequirements.isPresent()) {
                return false;
            }
            return maybeActualJobResourceRequirements
                    .map(actual -> actual.equals(maybeJobResourceRequirements.get()))
                    .orElse(true);
        }

        @Override
        public void describeTo(Description description) {}
    }
}
