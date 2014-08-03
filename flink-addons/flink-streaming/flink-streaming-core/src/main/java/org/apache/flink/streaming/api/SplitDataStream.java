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

public class SplitDataStream<T> extends DataStream<T> {

	protected SplitDataStream(DataStream<T> dataStream) {
		super(dataStream);
	}

	/**
	 * Sets the output name for which the vertex will receive tuples from the
	 * preceding Directed stream
	 * 
	 * @param outputName
	 *            The output name for which the operator will receive the input.
	 * @return Returns the modified DataStream
	 */
	public NamedDataStream<T> select(String outputName) {
		NamedDataStream<T> returnStream = new NamedDataStream<T>(this);
		returnStream.userDefinedName = outputName;
		return returnStream;
	}

	@Override
	protected DataStream<T> copy() {
		return new SplitDataStream<T>(this);
	}

}
