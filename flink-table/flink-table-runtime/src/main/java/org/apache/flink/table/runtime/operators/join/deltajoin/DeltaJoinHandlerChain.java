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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A runtime chain for delta join to lookup tables.
 *
 * <p>It serves as the entry point that triggers multiple chained handlers when data arrives at the
 * stream side.
 */
public class DeltaJoinHandlerChain implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DeltaJoinHandlerBase head;
    private final int[] streamInputOrdinals;

    private transient MultiInputRowDataBuffer sharedMultiInputRowDataBuffer;

    private DeltaJoinHandlerChain(DeltaJoinHandlerBase head, int[] streamInputOrdinals) {
        this.head = head;

        this.streamInputOrdinals = streamInputOrdinals;

        Preconditions.checkArgument(streamInputOrdinals.length > 0);
    }

    /** Chain the handlers together to form a processing pipeline. */
    public static DeltaJoinHandlerChain build(
            List<DeltaJoinHandlerBase> handlers, int[] streamInputOrdinals) {
        Preconditions.checkArgument(!handlers.isEmpty());

        DeltaJoinHandlerBase header = null;
        for (int i = 0; i < handlers.size(); i++) {
            DeltaJoinHandlerBase handler = handlers.get(i);
            if (i == 0) {
                header = handler;
            }
            if (i != handlers.size() - 1) {
                handler.setNext(handlers.get(i + 1));
            }
        }

        return new DeltaJoinHandlerChain(header, streamInputOrdinals);
    }

    public void open(
            OpenContext openContext, DeltaJoinHandlerBase.DeltaJoinHandlerContext handlerContext)
            throws Exception {
        this.sharedMultiInputRowDataBuffer = handlerContext.getSharedMultiInputRowDataBuffer();

        head.open(openContext, handlerContext);
    }

    /** Start handing one input row from stream side to trigger the chained handlers. */
    public void asyncHandle(RowData input) throws Exception {
        prepareDataInBuffer(input);
        head.asyncHandle();
    }

    public DeltaJoinHandlerChain copy() {
        DeltaJoinHandlerBase newHead = head.copy();
        return new DeltaJoinHandlerChain(newHead, streamInputOrdinals);
    }

    public void reset() {
        head.reset();
    }

    public void close() throws Exception {
        head.close();
    }

    @VisibleForTesting
    public DeltaJoinHandlerBase getHead() {
        return head;
    }

    /**
     * Place the data arriving from the stream side into the {@link MultiInputRowDataBuffer}.
     *
     * <p>If the upstream of this stream side is directly a binary input, the data will be placed
     * into the single corresponding slot. For example, if we are processing data from the left
     * (stream) side A in {@code DeltaJoin1}, the data fills slot 0 only in {@link
     * MultiInputRowDataBuffer}.
     *
     * <p>If the upstream of this stream side is a delta join, the prepared data will represent the
     * dataset of binary inputs A and B after being processed by the upstream delta join, and it
     * will span multiple input ordinals in the buffer. For example, if we are processing data from
     * the left (stream) side {@code DeltaJoin1} in {@code DeltaJoin2}, the data will span the slots
     * 0 and 1 in {@link MultiInputRowDataBuffer}:
     *
     * <pre>{@code
     *         DeltaJoin2
     *       /              \
     *  DeltaJoin1       #2 C
     *    /      \
     * #0 A    #1 B
     * }</pre>
     */
    private void prepareDataInBuffer(RowData data) {
        sharedMultiInputRowDataBuffer.prepareDataInBuffer(
                data, Arrays.stream(streamInputOrdinals).boxed().collect(Collectors.toSet()));
    }
}
