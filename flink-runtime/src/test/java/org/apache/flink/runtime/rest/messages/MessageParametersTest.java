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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

/** Tests for {@link MessageParameters}. */
public class MessageParametersTest extends TestLogger {
    @Test
    public void testResolveUrl() {
        String genericUrl = "/jobs/:jobid/state";
        TestMessageParameters parameters = new TestMessageParameters();
        JobID pathJobID = new JobID();
        JobID queryJobID = new JobID();
        parameters.pathParameter.resolve(pathJobID);
        parameters.queryParameter.resolve(Collections.singletonList(queryJobID));

        String resolvedUrl = MessageParameters.resolveUrl(genericUrl, parameters);

        Assert.assertEquals("/jobs/" + pathJobID + "/state?jobid=" + queryJobID, resolvedUrl);
    }

    @Test
    public void testUnresolvedParameters() {
        String genericUrl = "/jobs/:jobid/state";
        TestMessageParameters parameters = new TestMessageParameters();
        try {
            MessageParameters.resolveUrl(genericUrl, parameters);
            Assert.fail();
        } catch (IllegalStateException expected) {
            // the mandatory jobid path parameter was not resolved
        }
        JobID jobID = new JobID();
        parameters.pathParameter.resolve(jobID);

        String resolvedUrl = MessageParameters.resolveUrl(genericUrl, parameters);
        Assert.assertEquals("/jobs/" + jobID + "/state", resolvedUrl);
    }

    private static class TestMessageParameters extends MessageParameters {
        private final TestPathParameter pathParameter = new TestPathParameter();
        private final TestQueryParameter queryParameter = new TestQueryParameter();

        @Override
        public Collection<MessagePathParameter<?>> getPathParameters() {
            return Collections.singleton(pathParameter);
        }

        @Override
        public Collection<MessageQueryParameter<?>> getQueryParameters() {
            return Collections.singleton(queryParameter);
        }
    }

    private static class TestPathParameter extends MessagePathParameter<JobID> {

        TestPathParameter() {
            super("jobid");
        }

        @Override
        public JobID convertFromString(String value) {
            return JobID.fromHexString(value);
        }

        @Override
        protected String convertToString(JobID value) {
            return value.toString();
        }

        @Override
        public String getDescription() {
            return "path parameter";
        }
    }

    private static class TestQueryParameter extends MessageQueryParameter<JobID> {

        TestQueryParameter() {
            super("jobid", MessageParameterRequisiteness.OPTIONAL);
        }

        @Override
        public JobID convertStringToValue(String value) {
            return JobID.fromHexString(value);
        }

        @Override
        public String convertValueToString(JobID value) {
            return value.toString();
        }

        @Override
        public String getDescription() {
            return "query parameter";
        }
    }
}
