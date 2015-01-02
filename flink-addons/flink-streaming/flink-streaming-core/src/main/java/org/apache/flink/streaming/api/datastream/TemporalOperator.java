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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.streaming.api.windowing.helper.SystemTimestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

public abstract class TemporalOperator<I1, I2, OP> {

	public final DataStream<I1> input1;
	public final DataStream<I2> input2;

	public long windowSize;
	public long slideInterval;

	public TimestampWrapper<I1> timeStamp1;
	public TimestampWrapper<I2> timeStamp2;

	public TemporalOperator(DataStream<I1> input1, DataStream<I2> input2) {
		if (input1 == null || input2 == null) {
			throw new NullPointerException();
		}
		this.input1 = input1.copy();
		this.input2 = input2.copy();
	}

	/**
	 * Continues a temporal transformation.<br/>
	 * Defines the window size on which the two DataStreams will be transformed.
	 * 
	 * @param windowSize
	 *            The size of the window in milliseconds.
	 * @return An incomplete temporal transformation.
	 */
	public OP onWindow(long windowSize) {
		return onWindow(windowSize, windowSize);
	}

	/**
	 * Continues a temporal transformation.<br/>
	 * Defines the window size on which the two DataStreams will be transformed.
	 * 
	 * @param windowSize
	 *            The size of the window in milliseconds.
	 * @param slideInterval
	 *            The slide size of the window.
	 * @return An incomplete temporal transformation.
	 */
	@SuppressWarnings("unchecked")
	public OP onWindow(long windowSize, long slideInterval) {
		return onWindow(windowSize, slideInterval,
				(TimestampWrapper<I1>) SystemTimestamp.getWrapper(),
				(TimestampWrapper<I2>) SystemTimestamp.getWrapper());
	}

	/**
	 * Continues a temporal transformation.<br/>
	 * Defines the window size on which the two DataStreams will be transformed.
	 * 
	 * @param windowSize
	 *            The size of the window in milliseconds.
	 * @param slideInterval
	 *            The slide size of the window.
	 * @param timeStamp1
	 *            The timestamp used to extract time from the elements of the
	 *            first data stream.
	 * @param timeStamp2
	 *            The timestamp used to extract time from the elements of the
	 *            second data stream.
	 * @return An incomplete temporal transformation.
	 */
	public OP onWindow(long windowSize, long slideInterval, TimestampWrapper<I1> timeStamp1,
			TimestampWrapper<I2> timeStamp2) {

		this.windowSize = windowSize;
		this.slideInterval = slideInterval;

		this.timeStamp1 = timeStamp1;
		this.timeStamp2 = timeStamp2;

		return createNextWindowOperator();
	}

	protected abstract OP createNextWindowOperator();

}
