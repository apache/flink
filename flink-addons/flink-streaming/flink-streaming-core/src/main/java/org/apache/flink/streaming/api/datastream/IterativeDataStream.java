/**
 *
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
 *
 */

package org.apache.flink.streaming.api.datastream;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.partitioner.ForwardPartitioner;

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
	 * program part. By default the DataStream represented by the parameter will
	 * be fed back to the iteration head, however the user can explicitly select
	 * which tuples should be iterated by {@code directTo(OutputSelector)}.
	 * Tuples directed to 'iterate' will be fed back to the iteration head.
	 * 
	 * @param iterationResult
	 *            The data stream that can be fed back to the next iteration.
	 * 
	 */
	public DataStream<IN> closeWith(DataStream<IN> iterationResult) {
		return closeWith(iterationResult, "iterate");
	}

	/**
	 * Closes the iteration. This method defines the end of the iterative
	 * program part. By default the DataStream represented by the parameter will
	 * be fed back to the iteration head, however the user can explicitly select
	 * which tuples should be iterated by {@code directTo(OutputSelector)}.
	 * Tuples directed to 'iterate' will be fed back to the iteration head.
	 * 
	 * @param iterationTail
	 *            The data stream that can be fed back to the next iteration.
	 * @param iterationName
	 *            Name of the iteration edge (backward edge to iteration head)
	 *            when used with directed emits
	 * 
	 */
	public <R> DataStream<IN> closeWith(DataStream<IN> iterationTail, String iterationName) {
		DataStream<R> returnStream = new DataStreamSink<R>(environment, "iterationSink");

		jobGraphBuilder.addIterationSink(returnStream.getId(), iterationTail.getId(),
				iterationID.toString(), iterationTail.getParallelism(), waitTime);

		jobGraphBuilder.setIterationSourceSettings(iterationID.toString(), iterationTail.getId());

		List<String> name = Arrays.asList(new String[] { iterationName });

		if (iterationTail instanceof MergedDataStream) {
			for (DataStream<IN> stream : ((MergedDataStream<IN>) iterationTail).mergedStreams) {
				String inputID = stream.getId();
				jobGraphBuilder.setEdge(inputID, returnStream.getId(),
						new ForwardPartitioner<IN>(), 0, name);
			}
		} else {

			jobGraphBuilder.setEdge(iterationTail.getId(), returnStream.getId(),
					new ForwardPartitioner<IN>(), 0, name);
		}

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