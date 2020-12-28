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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * A stream operator that emits watermark in a given event-time interval. This mini-batch assigner
 * works in event time, which means the watermark is forwarded from upstream but filtered by the
 * event-time interval. The downstream operators (e.g. windows) will trigger mini-batch once the
 * received watermark is advanced. So only the watermarks that across the event-time interval
 * boundary (i.e. window-end) will be forwarded. This is for windows to have most efficient
 * mini-batch.
 *
 * <p>For example, if this operator receives watermarks {@code 0,1,2,3 ..., 19, 20, 21}. The
 * downstream operator is a 5-size window aggregate, then the mini-batch interval is 5 (this is
 * inferred by rules). Then only {@code 4, 9, 14, 19} will be forwarded, because they are the
 * watermarks trigger windows.
 *
 * <p>The difference between this operator and {@link ProcTimeMiniBatchAssignerOperator} is that,
 * this operator forwards watermarks from upstream, but the other generates watermarks by itself
 * using processing time.
 */
public class RowTimeMiniBatchAssginerOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** The event-time interval for emitting watermarks. */
    private final long minibatchInterval;

    /** Current watermark of this operator, but may not be emitted. */
    private transient long currentWatermark;

    /** The next watermark to be emitted. */
    private transient long nextWatermark;

    public RowTimeMiniBatchAssginerOperator(long minibatchInterval) {
        this.minibatchInterval = minibatchInterval;
    }

    @Override
    public void open() throws Exception {
        super.open();

        currentWatermark = 0;
        nextWatermark =
                getMiniBatchStart(currentWatermark, minibatchInterval) + minibatchInterval - 1;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        // forward records
        output.collect(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // if we receive a Long.MAX_VALUE watermark we forward it since it is used
        // to signal the end of input and to not block watermark progress downstream
        if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
            currentWatermark = Long.MAX_VALUE;
            output.emitWatermark(mark);
            return;
        }

        currentWatermark = Math.max(currentWatermark, mark.getTimestamp());
        if (currentWatermark >= nextWatermark) {
            advanceWatermark();
        }
    }

    private void advanceWatermark() {
        output.emitWatermark(new Watermark(currentWatermark));
        long start = getMiniBatchStart(currentWatermark, minibatchInterval);
        long end = start + minibatchInterval - 1;
        nextWatermark = end > currentWatermark ? end : end + minibatchInterval;
    }

    @Override
    public void close() throws Exception {
        super.close();

        // emit the buffered watermark
        advanceWatermark();
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------
    /** Method to get the mini-batch start for a watermark. */
    private static long getMiniBatchStart(long watermark, long interval) {
        return watermark - (watermark + interval) % interval;
    }
}
