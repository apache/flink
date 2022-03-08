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

package org.apache.flink.state.api.input;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.input.splits.PrioritizedOperatorSubtaskStateInputSplit;
import org.apache.flink.state.api.utils.CustomStateBackendFactory;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for the stream operator context builder. */
public class StreamOperatorContextBuilderTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(StreamOperatorContextBuilderTest.class);

    @Test(expected = CustomStateBackendFactory.ExpectedException.class)
    public void testStateBackendLoading() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                StateBackendOptions.STATE_BACKEND,
                CustomStateBackendFactory.class.getCanonicalName());

        StreamOperatorContextBuilder builder =
                new StreamOperatorContextBuilder(
                        new MockStreamingRuntimeContext(true, 1, 0),
                        configuration,
                        new OperatorState(new OperatorID(), 1, 128),
                        new PrioritizedOperatorSubtaskStateInputSplit() {
                            @Override
                            public PrioritizedOperatorSubtaskState
                                    getPrioritizedOperatorSubtaskState() {
                                return PrioritizedOperatorSubtaskState.emptyNotRestored();
                            }

                            @Override
                            public int getSplitNumber() {
                                return 0;
                            }
                        },
                        new CloseableRegistry(),
                        null);

        builder.build(LOG);
    }
}
