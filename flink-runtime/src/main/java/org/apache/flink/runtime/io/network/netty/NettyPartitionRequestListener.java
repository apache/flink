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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of {@link PartitionRequestListener} for netty partition request. */
public class NettyPartitionRequestListener implements PartitionRequestListener {
    private final ResultPartitionProvider resultPartitionProvider;
    private final NetworkSequenceViewReader reader;
    private final int subPartitionIndex;
    private final ResultPartitionID resultPartitionId;
    private final long createTimestamp;

    public NettyPartitionRequestListener(
            ResultPartitionProvider resultPartitionProvider,
            NetworkSequenceViewReader reader,
            int subPartitionIndex,
            ResultPartitionID resultPartitionId) {
        this(
                resultPartitionProvider,
                reader,
                subPartitionIndex,
                resultPartitionId,
                System.currentTimeMillis());
    }

    @VisibleForTesting
    public NettyPartitionRequestListener(
            ResultPartitionProvider resultPartitionProvider,
            NetworkSequenceViewReader reader,
            int subPartitionIndex,
            ResultPartitionID resultPartitionId,
            long createTimestamp) {
        this.resultPartitionProvider = resultPartitionProvider;
        this.reader = reader;
        this.subPartitionIndex = subPartitionIndex;
        this.resultPartitionId = resultPartitionId;
        this.createTimestamp = createTimestamp;
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
        return reader.getReceiverId();
    }

    @Override
    public void notifyPartitionCreated(ResultPartition partition) throws IOException {
        checkNotNull(partition);
        reader.notifySubpartitionCreated(partition, subPartitionIndex);
    }

    @Override
    public void notifyPartitionCreatedTimeout() {
        reader.notifyPartitionRequestTimeout(this);
    }

    @Override
    public void releaseListener() {
        resultPartitionProvider.releasePartitionRequestListener(this);
    }
}
