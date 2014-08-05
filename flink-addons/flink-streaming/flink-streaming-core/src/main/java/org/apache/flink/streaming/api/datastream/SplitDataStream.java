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

/**
 * The SplitDataStream represents an operator that has been split using an
 * {@link OutputSelector}. Named outputs can be selected using the
 * {@link #select} function.
 *
 * @param <T>
 *            The type of the output.
 */
public class SplitDataStream<T> {

	DataStream<T> dataStream;

	protected SplitDataStream(DataStream<T> dataStream) {
		this.dataStream = dataStream.copy();
	}

	/**
	 * Sets the output names for which the next operator will receive values.
	 * 
	 * @param outputNames
	 *            The output names for which the operator will receive the
	 *            input.
	 * @return Returns the modified DataStream
	 */
	public DataStream<T> select(String... outputNames) {
		DataStream<T> returnStream = selectOutput(outputNames[0]);
		for (int i = 1; i < outputNames.length; i++) {
			if (outputNames[i] == "") {
				throw new IllegalArgumentException("User defined name must not be empty string");
			}

			returnStream = connectWithNames(returnStream, selectOutput(outputNames[i]));
		}
		return returnStream;
	}

	private DataStream<T> connectWithNames(DataStream<T> stream1, DataStream<T> stream2) {
		ConnectedDataStream<T> returnStream = new ConnectedDataStream<T>(stream1.copy());
		returnStream.connectedStreams.add(stream2.copy());
		return returnStream;
	}

	private DataStream<T> selectOutput(String outputName) {
		DataStream<T> returnStream = dataStream.copy();
		returnStream.userDefinedName = outputName;
		return returnStream;
	}

}
