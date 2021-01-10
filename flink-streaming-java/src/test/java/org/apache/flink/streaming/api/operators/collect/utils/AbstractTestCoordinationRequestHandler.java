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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;
import org.apache.flink.util.OptionalFailure;

import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** A {@link CoordinationRequestHandler} to test fetching SELECT query results. */
public abstract class AbstractTestCoordinationRequestHandler<T>
        implements CoordinationRequestHandler {

    protected static final int BATCH_SIZE = 3;

    protected final TypeSerializer<T> serializer;
    protected final String accumulatorName;

    protected LinkedList<T> buffered;
    protected String version;
    protected long offset;
    protected long checkpointedOffset;

    private final Map<String, OptionalFailure<Object>> accumulatorResults;

    protected final Random random;
    protected boolean closed;

    public AbstractTestCoordinationRequestHandler(
            TypeSerializer<T> serializer, String accumulatorName) {
        this.serializer = serializer;
        this.accumulatorName = accumulatorName;

        this.buffered = new LinkedList<>();
        this.version = UUID.randomUUID().toString();
        this.offset = 0;
        this.checkpointedOffset = 0;

        this.accumulatorResults = new HashMap<>();

        this.random = new Random();
        this.closed = false;
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        if (closed) {
            throw new RuntimeException("Handler closed");
        }

        Assert.assertTrue(request instanceof CollectCoordinationRequest);
        CollectCoordinationRequest collectRequest = (CollectCoordinationRequest) request;

        updateBufferedResults();
        Assert.assertTrue(offset <= collectRequest.getOffset());

        List<T> subList = Collections.emptyList();
        if (collectRequest.getVersion().equals(version)) {
            while (buffered.size() > 0 && collectRequest.getOffset() > offset) {
                buffered.removeFirst();
                offset++;
            }
            subList = new ArrayList<>();
            Iterator<T> iterator = buffered.iterator();
            for (int i = 0; i < BATCH_SIZE && iterator.hasNext(); i++) {
                subList.add(iterator.next());
            }
        }
        List<byte[]> nextBatch = CollectTestUtils.toBytesList(subList, serializer);

        CoordinationResponse response;
        if (random.nextBoolean()) {
            // with 50% chance we return valid result
            response = new CollectCoordinationResponse(version, checkpointedOffset, nextBatch);
        } else {
            // with 50% chance we return invalid result
            response =
                    new CollectCoordinationResponse(
                            collectRequest.getVersion(), -1, Collections.emptyList());
        }
        return CompletableFuture.completedFuture(response);
    }

    protected abstract void updateBufferedResults();

    public boolean isClosed() {
        return closed;
    }

    public Map<String, OptionalFailure<Object>> getAccumulatorResults() {
        return accumulatorResults;
    }

    protected void buildAccumulatorResults() {
        List<byte[]> finalResults = CollectTestUtils.toBytesList(buffered, serializer);
        SerializedListAccumulator<byte[]> listAccumulator = new SerializedListAccumulator<>();
        try {
            byte[] serializedResult =
                    CollectSinkFunction.serializeAccumulatorResult(
                            offset, version, checkpointedOffset, finalResults);
            listAccumulator.add(serializedResult, BytePrimitiveArraySerializer.INSTANCE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        accumulatorResults.put(
                accumulatorName, OptionalFailure.of(listAccumulator.getLocalValue()));
    }
}
