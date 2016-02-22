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

/**
 * The {@code AssignerWithPunctuatedWatermarks} assigns event time timestamps to elements,
 * and generates low watermarks that signal event time progress within the stream.
 * 
 * <p>Use these class if certain special elements act as markers that signify event time
 * progress, and when you want to emit watermarks specifically at certain events.
 * 
 * <p>For use cases that should periodically emit watermarks based on element timestamps,
 * use the {@link AssignerWithPeriodicWatermarks} instead.
 *
 * <p>The following example illustrates how to use this timestamp extractor and watermark
 * generator. It assumes elements carry a timestamp that describes when they were created,
 * and that some elements carry a flag, marking them as the end of a sequence such that no
 * elements with smaller timestamps can come any more.
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
public interface AssignerWithPunctuatedWatermarks<T> extends TimestampAssigner<T> {
	
	/**
	 * Asks this implementation if it wants to emit a watermark. This method is called right after
	 * the {@link #extractTimestamp(Object, long)} method. If the method returns a positive
	 * value, a new watermark should be emitted. If a negative value is emitted, no new watermark
	 * will be generated.
	 * 
	 * <p>Note that whenever this method returns a positive value that is larger than the previous
	 * value, a new watermark is generated. Hence, the implementation has full control how often
	 * watermarks are generated.
	 * 
	 * <p>For an example how to use this method, see the documentation of
	 * {@link AssignerWithPunctuatedWatermarks this class}.
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp);
}
