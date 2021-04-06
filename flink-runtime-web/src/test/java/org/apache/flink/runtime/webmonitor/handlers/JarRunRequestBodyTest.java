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
import org.apache.flink.runtime.rest.messages.RestRequestMarshallingTestBase;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/** Tests for {@link JarRunRequestBody}. */
public class JarRunRequestBodyTest extends RestRequestMarshallingTestBase<JarRunRequestBody> {

    @Override
    protected Class<JarRunRequestBody> getTestRequestClass() {
        return JarRunRequestBody.class;
    }

    @Override
    protected JarRunRequestBody getTestRequestInstance() {
        return new JarRunRequestBody(
                "hello", "world", Arrays.asList("boo", "far"), 4, new JobID(), true, "foo/bar");
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            final JarRunRequestBody expected, final JarRunRequestBody actual) {
        assertEquals(expected.getEntryClassName(), actual.getEntryClassName());
        assertEquals(expected.getProgramArguments(), actual.getProgramArguments());
        assertEquals(expected.getProgramArgumentsList(), actual.getProgramArgumentsList());
        assertEquals(expected.getParallelism(), actual.getParallelism());
        assertEquals(expected.getJobId(), actual.getJobId());
        assertEquals(expected.getAllowNonRestoredState(), actual.getAllowNonRestoredState());
        assertEquals(expected.getSavepointPath(), actual.getSavepointPath());
    }
}
