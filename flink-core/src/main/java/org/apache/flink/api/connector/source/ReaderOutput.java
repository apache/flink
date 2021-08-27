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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;

/**
 * The interface provided by the Flink runtime to the {@link SourceReader} to emit records, and
 * optionally watermarks, to downstream operators for message processing.
 *
 * <p>The {@code ReaderOutput} is a {@link SourceOutput} and can be used directly to emit the stream
 * of events from the source. This is recommended for source where the SourceReader processes only a
 * single split, or where NO split-specific characteristics are required (like per-split watermarks
 * and idleness, split-specific event-time skew handling, etc.). As a special case, this is true for
 * sources that are purely supporting bounded/batch data processing.
 *
 * <p>For most streaming sources, the {@code SourceReader} should use split-specific outputs, to
 * allow the processing logic to run per-split watermark generators, idleness detection, etc. To
 * create a split-specific {@code SourceOutput} use the {@link
 * ReaderOutput#createOutputForSplit(String)} method, using the Source Split's ID. Make sure to
 * release the output again once the source has finished processing that split.
 */
@PublicEvolving
public interface ReaderOutput<T> extends SourceOutput<T> {

    /**
     * Emit a record without a timestamp.
     *
     * <p>Use this method if the source system does not have a notion of records with timestamps.
     *
     * <p>The events later pass through a {@link TimestampAssigner}, which attaches a timestamp to
     * the event based on the event's contents. For example a file source with JSON records would
     * not have a generic timestamp from the file reading and JSON parsing process, and thus use
     * this method to produce initially a record without a timestamp. The {@code TimestampAssigner}
     * in the next step would be used to extract timestamp from a field of the JSON object.
     *
     * @param record the record to emit.
     */
    @Override
    void collect(T record);

    /**
     * Emit a record with a timestamp.
     *
     * <p>Use this method if the source system has timestamps attached to records. Typical examples
     * would be Logs, PubSubs, or Message Queues, like Kafka or Kinesis, which store a timestamp
     * with each event.
     *
     * <p>The events typically still pass through a {@link TimestampAssigner}, which may decide to
     * either use this source-provided timestamp, or replace it with a timestamp stored within the
     * event (for example if the event was a JSON object one could configure aTimestampAssigner that
     * extracts one of the object's fields and uses that as a timestamp).
     *
     * @param record the record to emit.
     * @param timestamp the timestamp of the record.
     */
    @Override
    void collect(T record, long timestamp);

    /**
     * Emits the given watermark.
     *
     * <p>Emitting a watermark also implicitly marks the stream as <i>active</i>, ending previously
     * marked idleness.
     */
    @Override
    void emitWatermark(Watermark watermark);

    /**
     * Marks this output as idle, meaning that downstream operations do not wait for watermarks from
     * this output.
     *
     * <p>An output becomes active again as soon as the next watermark is emitted.
     */
    @Override
    void markIdle();

    /**
     * Creates a {@code SourceOutput} for a specific Source Split. Use these outputs if you want to
     * run split-local logic, like watermark generation.
     *
     * <p>If a split-local output was already created for this split-ID, the method will return that
     * instance, so that only one split-local output exists per split-ID.
     *
     * <p><b>IMPORTANT:</b> After the split has been finished, it is crucial to release the created
     * output again. Otherwise it will continue to contribute to the watermark generation like a
     * perpetually stalling source split, and may hold back the watermark indefinitely.
     *
     * @see #releaseOutputForSplit(String)
     */
    SourceOutput<T> createOutputForSplit(String splitId);

    /**
     * Releases the {@code SourceOutput} created for the split with the given ID.
     *
     * @see #createOutputForSplit(String)
     */
    void releaseOutputForSplit(String splitId);
}
