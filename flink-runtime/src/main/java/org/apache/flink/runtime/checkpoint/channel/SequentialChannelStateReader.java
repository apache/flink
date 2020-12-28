package org.apache.flink.runtime.checkpoint.channel;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

/** Reads channel state saved during checkpoint/savepoint. */
@Internal
public interface SequentialChannelStateReader extends AutoCloseable {

    void readInputData(InputGate[] inputGates) throws IOException, InterruptedException;

    void readOutputData(ResultPartitionWriter[] writers, boolean notifyAndBlockOnCompletion)
            throws IOException, InterruptedException;

    @Override
    void close() throws Exception;

    SequentialChannelStateReader NO_OP =
            new SequentialChannelStateReader() {

                @Override
                public void readInputData(InputGate[] inputGates) {}

                @Override
                public void readOutputData(
                        ResultPartitionWriter[] writers, boolean notifyAndBlockOnCompletion) {}

                @Override
                public void close() {}
            };
}
