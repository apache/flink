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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * An iterator which iterates through the results of a query job.
 *
 * <p>The behavior of the iterator is slightly different under different checkpointing mode.
 *
 * <ul>
 *   <li>If the user does not specify any checkpointing, results are immediately delivered but
 *       exceptions will be thrown when the job restarts.
 *   <li>If the user specifies exactly-once checkpointing, results are guaranteed to be exactly-once
 *       but they're only visible after the corresponding checkpoint completes.
 *   <li>If the user specifies at-least-once checkpointing, results are immediately delivered but
 *       the same result may be delivered multiple times.
 * </ul>
 *
 * <p>NOTE: After using this iterator, the close method MUST be called in order to release job
 * related resources.
 */
public class CollectResultIterator<T> implements CloseableIterator<T> {

    private final CollectResultFetcher<T> fetcher;
    private T bufferedResult;

    public CollectResultIterator(
            CompletableFuture<OperatorID> operatorIdFuture,
            TypeSerializer<T> serializer,
            String accumulatorName,
            CheckpointConfig checkpointConfig) {
        AbstractCollectResultBuffer<T> buffer = createBuffer(serializer, checkpointConfig);
        this.fetcher = new CollectResultFetcher<>(buffer, operatorIdFuture, accumulatorName);
        this.bufferedResult = null;
    }

    @VisibleForTesting
    public CollectResultIterator(
            AbstractCollectResultBuffer<T> buffer,
            CompletableFuture<OperatorID> operatorIdFuture,
            String accumulatorName,
            int retryMillis) {
        this.fetcher =
                new CollectResultFetcher<>(buffer, operatorIdFuture, accumulatorName, retryMillis);
        this.bufferedResult = null;
    }

    @Override
    public boolean hasNext() {
        // we have to make sure that the next result exists
        // it is possible that there is no more result but the job is still running
        if (bufferedResult == null) {
            bufferedResult = nextResultFromFetcher();
        }
        return bufferedResult != null;
    }

    @Override
    public T next() {
        if (bufferedResult == null) {
            bufferedResult = nextResultFromFetcher();
        }
        T ret = bufferedResult;
        bufferedResult = null;
        return ret;
    }

    @Override
    public void close() throws Exception {
        fetcher.close();
    }

    public void setJobClient(JobClient jobClient) {
        fetcher.setJobClient(jobClient);
    }

    private T nextResultFromFetcher() {
        try {
            return fetcher.next();
        } catch (IOException e) {
            fetcher.close();
            throw new RuntimeException("Failed to fetch next result", e);
        }
    }

    private AbstractCollectResultBuffer<T> createBuffer(
            TypeSerializer<T> serializer, CheckpointConfig checkpointConfig) {
        if (checkpointConfig.isCheckpointingEnabled()) {
            if (checkpointConfig.getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE) {
                return new CheckpointedCollectResultBuffer<>(serializer);
            } else {
                return new UncheckpointedCollectResultBuffer<>(serializer, true);
            }
        } else {
            return new UncheckpointedCollectResultBuffer<>(serializer, false);
        }
    }
}
