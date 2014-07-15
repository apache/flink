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

package org.apache.flink.streaming.api;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * The iterative data stream represents the start of an iteration in a
 * DataStream.
 * 
 * @param <T>
 */
public class IterativeDataStream<T extends Tuple> extends StreamOperator<T, T> {

	static Integer iterationCount = 0;

	protected IterativeDataStream(DataStream<T> dataStream) {
		super(dataStream.environment, "iteration", dataStream.id);
		iterationID = iterationCount;
		iterationCount++;
		iterationflag = true;
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
	public DataStream<T> closeWith(DataStream<T> iterationResult) {
		environment.addIterationSink(iterationResult, iterationID.toString());
		return iterationResult;
	}
	
	

}