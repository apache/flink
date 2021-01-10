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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

/** Tests the uid translation to {@link org.apache.flink.runtime.jobgraph.OperatorID}. */
@SuppressWarnings("serial")
public class GetOperatorUniqueIDTest extends TestLogger {

    /**
     * If expected values ever change double check that the change is not braking the contract of
     * {@link StreamingRuntimeContext#getOperatorUniqueID()} being stable between job submissions.
     */
    @Test
    public void testGetOperatorUniqueID() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.fromElements(1, 2, 3)
                .map(new VerifyOperatorIDMapFunction("6c4f323f22da8fb6e34f80c61be7a689"))
                .uid("42")
                .map(new VerifyOperatorIDMapFunction("3e129e83691e7737fbf876b47452acbc"))
                .uid("44");

        env.execute();
    }

    private static class VerifyOperatorIDMapFunction extends AbstractRichFunction
            implements MapFunction<Integer, Integer> {
        private static final long serialVersionUID = 6584823409744624276L;

        private final String expectedOperatorUniqueID;

        public VerifyOperatorIDMapFunction(String expectedOperatorUniqueID) {
            this.expectedOperatorUniqueID = checkNotNull(expectedOperatorUniqueID);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            assertEquals(
                    expectedOperatorUniqueID,
                    ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID());
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }
}
