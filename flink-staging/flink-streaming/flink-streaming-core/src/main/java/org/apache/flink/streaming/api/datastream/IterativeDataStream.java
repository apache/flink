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
	
	protected IterativeDataStream(DataStream<IN> dataStream, long maxWaitTime) {
		super(dataStream);
		setBufferTimeout(dataStream.environment.getBufferTimeout());
		iterationID = iterationCount;
		iterationCount++;
		iterationWaitTime = maxWaitTime;
	}

	/**
	 * Closes the iteration. This method defines the end of the iterative
	 * program part that will be fed back to the start of the iteration. </br>
	 * </br>A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link DataStream#split(org.apache.flink.streaming.api.collector.selector.OutputSelector)}
	 * for more information.
	 * 
	 * 
	 * @param iterationTail
	 *            The data stream that is fed back to the next iteration head.
	 * @return Returns the stream that was fed back to the iteration. In most
	 *         cases no further transformation are applied on this stream.
	 * 
	 */
	public DataStream<IN> closeWith(DataStream<IN> iterationTail) {
		DataStream<IN> iterationSink = new DataStreamSink<IN>(environment, "Iteration Sink", null,
				null);

		// We add an iteration sink to the tail which will send tuples to the
		// iteration head
		streamGraph.addIterationTail(iterationSink.getId(), iterationTail.getId(), iterationID,
				iterationWaitTime);

		connectGraph(iterationTail.forward(), iterationSink.getId(), 0);
		return iterationTail;
	}
}
