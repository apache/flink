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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.runtime.rest.messages.RestRequestMarshallingTestBase;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.stop.StopWithSavepointRequestBody;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/** Tests for {@link StopWithSavepointRequestBody}. */
@RunWith(Parameterized.class)
public class StopWithSavepointTriggerRequestBodyTest
        extends RestRequestMarshallingTestBase<StopWithSavepointRequestBody> {

    private final StopWithSavepointRequestBody savepointTriggerRequestBody;

    public StopWithSavepointTriggerRequestBodyTest(
            final StopWithSavepointRequestBody savepointTriggerRequestBody) {
        this.savepointTriggerRequestBody = savepointTriggerRequestBody;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {new StopWithSavepointRequestBody("/tmp", true, null)},
                    {new StopWithSavepointRequestBody("/tmp", false, null)},
                    {new StopWithSavepointRequestBody("/tmp", true, new TriggerId())},
                    {new StopWithSavepointRequestBody("/tmp", false, new TriggerId())},
                });
    }

    @Override
    protected Class<StopWithSavepointRequestBody> getTestRequestClass() {
        return StopWithSavepointRequestBody.class;
    }

    @Override
    protected StopWithSavepointRequestBody getTestRequestInstance() {
        return savepointTriggerRequestBody;
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            final StopWithSavepointRequestBody expected,
            final StopWithSavepointRequestBody actual) {
        assertEquals(expected.getTargetDirectory(), actual.getTargetDirectory());
        assertEquals(expected.getTriggerId(), actual.getTriggerId());
        assertEquals(expected.shouldDrain(), actual.shouldDrain());
    }
}
