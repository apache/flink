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

package org.apache.flink.streaming.api.datastream;

/**
 * The iterative data stream represents the start of an iteration in a
 * {@link DataStream}.
 * 
 * @param <IN>
 *            Type of the DataStream
 */
public class IterativeDataStream<IN> extends
		SingleOutputStreamOperator<IN, IterativeDataStream<IN>> {

	static Integer iterationCount = 0;
	protected Integer iterationID;
	protected long waitTime;

	protected IterativeDataStream(DataStream<IN> dataStream) {
		super(dataStream);
		setBufferTimeout(dataStream.environment.getBufferTimeout());
		iterationID = iterationCount;
		iterationCount++;
		waitTime = 0;
	}

	protected IterativeDataStream(DataStream<IN> dataStream, Integer iterationID, long waitTime) {
		super(dataStream);
		this.iterationID = iterationID;
		this.waitTime = waitTime;
	}

	/**
	 * Closes the iteration. This method defines the end of the iterative
	 * program part that will be fed back to the start of the iteration. </br>
	 * </br>A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link SingleOutputStreamOperator#split(OutputSelector)} for more
	 * information.
	 * 
	 * 
	 * @param iterationResult
	 *            The data stream that is fed back to the next iteration head.
	 * @return Returns the stream that was fed back to the iteration. In most
	 *         cases no further transformation are applied on this stream.
	 * 
	 */
	public DataStream<IN> closeWith(DataStream<IN> iterationTail) {
		DataStream<IN> iterationSink = new DataStreamSink<IN>(environment, "iterationSink", null);

		jobGraphBuilder.addIterationTail(iterationSink.getId(), iterationTail.getId(), iterationID,
				iterationTail.getParallelism(), waitTime);

		jobGraphBuilder.setIterationSourceSettings(iterationID.toString(), iterationTail.getId());
		connectGraph(iterationTail, iterationSink.getId(), 0);
		return iterationTail;
	}

	/**
	 * Sets the max waiting time for the next record before shutting down the
	 * stream. If not set, then the user needs to manually kill the process to
	 * stop.
	 * 
	 * @param waitTimeMillis
	 *            Max waiting time in milliseconds
	 * @return The modified DataStream.
	 */
	public IterativeDataStream<IN> setMaxWaitTime(long waitTimeMillis) {
		this.waitTime = waitTimeMillis;
		return this;
	}

	@Override
	protected IterativeDataStream<IN> copy() {
		return new IterativeDataStream<IN>(this, iterationID, waitTime);
	}
}
