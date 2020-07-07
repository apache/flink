/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * The {@code AssignerWithPunctuatedWatermarks} assigns event time timestamps to elements,
 * and generates low watermarks that signal event time progress within the stream.
 * These timestamps and watermarks are used by functions and operators that operate
 * on event time, for example event time windows.
 *
 * <p>Use this class if certain special elements act as markers that signify event time
 * progress, and when you want to emit watermarks specifically at certain events.
 * The system will generate a new watermark, if the probed value is non-null
 * and has a timestamp larger than that of the previous watermark (to preserve
 * the contract of ascending watermarks).
 *
 * <p>For use cases that should periodically emit watermarks based on element timestamps,
 * use the {@link AssignerWithPeriodicWatermarks} instead.
 *
 * <p>The following example illustrates how to use this timestamp extractor and watermark
 * generator. It assumes elements carry a timestamp that describes when they were created,
 * and that some elements carry a flag, marking them as the end of a sequence such that no
 * elements with smaller timestamps can come anymore.
 *
 * <pre>{@code
 * public class WatermarkOnFlagAssigner implements AssignerWithPunctuatedWatermarks<MyElement> {
 *
 *     public long extractTimestamp(MyElement element, long previousElementTimestamp) {
 *         return element.getSequenceTimestamp();
 *     }
 *
 *     public Watermark checkAndGetNextWatermark(MyElement lastElement, long extractedTimestamp) {
 *         return lastElement.isEndOfSequence() ? new Watermark(extractedTimestamp) : null;
 *     }
 * }
 * }</pre>
 *
 * <p>Timestamps and watermarks are defined as {@code longs} that represent the
 * milliseconds since the Epoch (midnight, January 1, 1970 UTC).
 * A watermark with a certain value {@code t} indicates that no elements with event
 * timestamps {@code x}, where {@code x} is lower or equal to {@code t}, will occur any more.
 *
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
@Deprecated
public interface AssignerWithPunctuatedWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Asks this implementation if it wants to emit a watermark. This method is called right after
	 * the {@link #extractTimestamp(Object, long)} method.
	 *
	 * <p>The returned watermark will be emitted only if it is non-null and its timestamp
	 * is larger than that of the previously emitted watermark (to preserve the contract of
	 * ascending watermarks). If a null value is returned, or the timestamp of the returned
	 * watermark is smaller than that of the last emitted one, then no new watermark will
	 * be generated.
	 *
	 * <p>For an example how to use this method, see the documentation of
	 * {@link AssignerWithPunctuatedWatermarks this class}.
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	@Nullable
	Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp);
}
