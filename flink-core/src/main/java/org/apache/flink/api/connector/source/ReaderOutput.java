/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.Watermark;

/**
 * The interface provided by Flink task to the {@link SourceReader} to emit records
 * to downstream operators for message processing.
 */
@PublicEvolving
public interface ReaderOutput<T> extends SourceOutput<T> {

	/**
	 * Emit a record without a timestamp. Equivalent to {@link #collect(Object, long) collect(timestamp, null)};
	 *
	 * @param record the record to emit.
	 */
	@Override
	void collect(T record);

	/**
	 * Emit a record with timestamp.
	 *
	 * @param record the record to emit.
	 * @param timestamp the timestamp of the record.
	 */
	@Override
	void collect(T record, long timestamp);

	/**
	 * Emits the given watermark.
	 *
	 * <p>Emitting a watermark also implicitly marks the stream as <i>active</i>, ending
	 * previously marked idleness.
	 */
	@Override
	void emitWatermark(Watermark watermark);

	/**
	 * Marks this output as idle, meaning that downstream operations do not
	 * wait for watermarks from this output.
	 *
	 * <p>An output becomes active again as soon as the next watermark is emitted.
	 */
	@Override
	void markIdle();

	/**
	 * Creates a {@code SourceOutput} for a specific Source Split. Use these outputs if you want to
	 * run split-local logic, like watermark generation.
	 *
	 * <p>If a split-local output was already created for this split-ID, the method will return that instance,
	 * so that only one split-local output exists per split-ID.
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
