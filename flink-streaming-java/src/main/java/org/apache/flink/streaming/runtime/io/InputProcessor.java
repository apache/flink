/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

/**
 * The interface Input processor.
 */
interface InputProcessor {

	/**
	 * Process record.
	 *
	 * @param streamRecord the stream record
	 * @param channelIndex the channel index
	 * @throws Exception the exception
	 */
	void processRecord(StreamRecord streamRecord, int channelIndex) throws Exception;

	/**
	 * Process latency marker.
	 *
	 * @param latencyMarker the latency marker
	 * @param channelIndex  the channel index
	 * @throws Exception the exception
	 */
	void processLatencyMarker(LatencyMarker latencyMarker, int channelIndex) throws Exception;

	/**
	 * Process watermark.
	 *
	 * @param watermark    the watermark
	 * @param channelIndex the channel index
	 * @throws Exception the exception
	 */
	void processWatermark(Watermark watermark, int channelIndex) throws Exception;

	/**
	 * Process stream status.
	 *
	 * @param streamStatus the stream status
	 * @param channelIndex the channel index
	 * @throws Exception the exception
	 */
	void processStreamStatus(StreamStatus streamStatus, int channelIndex) throws Exception;

	/**
	 * End input.
	 *
	 * @throws Exception the exception
	 */
	void endInput() throws Exception;

	/**
	 * Release.
	 *
	 * @throws Exception the exception
	 */
	void release() throws Exception;
}
