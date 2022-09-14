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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.runtime.rest.messages.TriggerId;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the marshalling of {@link AsynchronousOperationResult}. */
@RunWith(Parameterized.class)
public class AsynchronousOperationResultTest
        extends RestResponseMarshallingTestBase<AsynchronousOperationResult<TriggerId>> {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {AsynchronousOperationResult.inProgress()},
                    {AsynchronousOperationResult.completed(new TriggerId())}
                });
    }

    private final AsynchronousOperationResult<TriggerId> asynchronousOperationResult;

    public AsynchronousOperationResultTest(
            AsynchronousOperationResult<TriggerId> asynchronousOperationResult) {
        this.asynchronousOperationResult = asynchronousOperationResult;
    }

    @Override
    protected Class<AsynchronousOperationResult<TriggerId>> getTestResponseClass() {
        return (Class<AsynchronousOperationResult<TriggerId>>)
                (Class<?>) AsynchronousOperationResult.class;
    }

    @Override
    protected Collection<Class<?>> getTypeParameters() {
        return Collections.singleton(TriggerId.class);
    }

    @Override
    protected AsynchronousOperationResult<TriggerId> getTestResponseInstance() throws Exception {
        return asynchronousOperationResult;
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            AsynchronousOperationResult<TriggerId> expected,
            AsynchronousOperationResult<TriggerId> actual) {
        assertThat(actual.queueStatus().getId(), is(expected.queueStatus().getId()));
        assertThat(actual.resource(), is(expected.resource()));
    }
}
