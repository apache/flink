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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;

/** {@link PartitionRequestListener} implementation for testing purposes. */
public class TestingPartitionRequestListener implements PartitionRequestListener {
    private final long createTimestamp;
    private final ResultPartitionID resultPartitionId;
    private final InputChannelID inputChannelId;
    private final NetworkSequenceViewReader reader;

    private TestingPartitionRequestListener(
            ResultPartitionID resultPartitionId,
            InputChannelID inputChannelId,
            NetworkSequenceViewReader reader) {
        this.createTimestamp = System.currentTimeMillis();
        this.resultPartitionId = resultPartitionId;
        this.inputChannelId = inputChannelId;
        this.reader = reader;
    }

    @Override
    public long getCreateTimestamp() {
        return createTimestamp;
    }

    @Override
    public ResultPartitionID getResultPartitionId() {
        return resultPartitionId;
    }

    @Override
    public NetworkSequenceViewReader getViewReader() {
        return reader;
    }

    @Override
    public InputChannelID getReceiverId() {
        return inputChannelId;
    }

    @Override
    public void notifyPartitionCreated(ResultPartition partition) throws IOException {
        reader.notifySubpartitionCreated(partition, 0);
    }

    @Override
    public void notifyPartitionCreatedTimeout() {}

    @Override
    public void releaseListener() {}

    public static TestingPartitionRequestListenerBuilder newBuilder() {
        return new TestingPartitionRequestListenerBuilder();
    }

    /** Factory for {@link TestingPartitionRequestListener}. */
    public static class TestingPartitionRequestListenerBuilder {
        private ResultPartitionID resultPartitionId = new ResultPartitionID();
        private InputChannelID inputChannelId = new InputChannelID();
        private NetworkSequenceViewReader reader = null;

        public TestingPartitionRequestListenerBuilder setResultPartitionId(
                ResultPartitionID resultPartitionId) {
            this.resultPartitionId = resultPartitionId;
            return this;
        }

        public TestingPartitionRequestListenerBuilder setInputChannelId(
                InputChannelID inputChannelId) {
            this.inputChannelId = inputChannelId;
            return this;
        }

        public TestingPartitionRequestListenerBuilder setNetworkSequenceViewReader(
                NetworkSequenceViewReader reader) {
            this.reader = reader;
            return this;
        }

        public TestingPartitionRequestListener build() {
            return new TestingPartitionRequestListener(resultPartitionId, inputChannelId, reader);
        }
    }
}
