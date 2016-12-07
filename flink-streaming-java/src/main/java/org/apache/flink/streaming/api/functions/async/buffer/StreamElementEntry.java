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

package org.apache.flink.streaming.api.functions.async.buffer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.io.IOException;
import java.util.List;

/**
 * The base class for entries in the {@link AsyncCollectorBuffer}
 *
 * @param <OUT> Output data type
 */

@Internal
public interface StreamElementEntry<OUT>  {
	/**
	 * Get result. Throw IOException while encountering an error.
	 *
	 * @return A List of result.
	 * @throws IOException IOException wrapping errors from user codes.
	 */
	List<OUT> getResult() throws IOException;

	/**
	 * Set the internal flag, marking the async operator has finished.
	 */
	void markDone();

	/**
	 * Get the flag indicating the async operator has finished or not.
	 *
	 * @return True for finished async operator.
	 */
	boolean isDone();

	/**
	 * Check inner element is StreamRecord or not.
	 *
	 * @return True if element is StreamRecord.
     */
	boolean isStreamRecord();

	/**
	 * Check inner element is Watermark or not.
	 *
	 * @return True if element is Watermark.
     */
	boolean isWatermark();

	/**
	 * Check inner element is LatencyMarker or not.
	 *
	 * @return True if element is LatencyMarker.
     */
	boolean isLatencyMarker();

	/**
	 * Get inner stream element.
	 *
	 * @return Inner {@link StreamElement}.
     */
	StreamElement getStreamElement();
}
