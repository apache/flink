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

import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;

public abstract class WindowDBOperator<I1, I2, OP> {

	protected final DataStream<I1> input1;
	protected final DataStream<I2> input2;

	long windowSize;
	long slideInterval;

	TimeStamp<I1> timeStamp1;
	TimeStamp<I2> timeStamp2;

	public WindowDBOperator(DataStream<I1> input1, DataStream<I2> input2) {
		if (input1 == null || input2 == null) {
			throw new NullPointerException();
		}
		this.input1 = input1.copy();
		this.input2 = input2.copy();
	}

	/**
	 * Continues a temporal Join transformation.<br/>
	 * Defines the window size on which the two DataStreams will be joined.
	 * 
	 * @param windowSize
	 *            The size of the window in milliseconds.
	 * @return An incomplete Join transformation. Call {@link JoinWindow#where}
	 *         to continue the Join.
	 */
	public OP onWindow(long windowSize) {
		return onWindow(windowSize, windowSize);
	}

	/**
	 * Continues a temporal Join transformation.<br/>
	 * Defines the window size on which the two DataStreams will be joined.
	 * 
	 * @param windowSize
	 *            The size of the window in milliseconds.
	 * @param slideInterval
	 *            The slide size of the window.
	 * @return An incomplete Join transformation. Call {@link JoinWindow#where}
	 *         to continue the Join.
	 */
	public OP onWindow(long windowSize, long slideInterval) {
		return onWindow(windowSize, slideInterval, new DefaultTimeStamp<I1>(),
				new DefaultTimeStamp<I2>());
	}

	/**
	 * Continues a temporal Join transformation.<br/>
	 * Defines the window size on which the two DataStreams will be joined.
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
	 * @return An incomplete Join transformation. Call {@link JoinWindow#where}
	 *         to continue the Join.
	 */
	public OP onWindow(long windowSize, long slideInterval, TimeStamp<I1> timeStamp1,
			TimeStamp<I2> timeStamp2) {

		this.windowSize = windowSize;
		this.slideInterval = slideInterval;

		this.timeStamp1 = timeStamp1;
		this.timeStamp2 = timeStamp2;

		return createNextWindowOperator();
	}
	
	protected abstract OP createNextWindowOperator();

}
