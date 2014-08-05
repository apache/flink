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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.partitioner.StreamPartitioner;

/**
 * The ConnectedDataStream represents a DataStream which consists of connected
 * outputs of DataStreams of the same type. Operators applied on this will
 * transform all the connected outputs jointly.
 *
 * @param <OUT>
 *            Type of the output.
 */
public class ConnectedDataStream<OUT> extends DataStream<OUT> {

	protected List<DataStream<OUT>> connectedStreams;

	protected ConnectedDataStream(StreamExecutionEnvironment environment, String operatorType) {
		super(environment, operatorType);
		this.connectedStreams = new ArrayList<DataStream<OUT>>();
		this.connectedStreams.add(this);
	}

	protected ConnectedDataStream(DataStream<OUT> dataStream) {
		super(dataStream);
		connectedStreams = new ArrayList<DataStream<OUT>>();
		if (dataStream instanceof ConnectedDataStream) {
			for (DataStream<OUT> stream : ((ConnectedDataStream<OUT>) dataStream).connectedStreams) {
				connectedStreams.add(stream);
			}
		} else {
			this.connectedStreams.add(this);
		}

	}

	// @Override
	// public IterativeDataStream<OUT> iterate() {
	// throw new RuntimeException("Cannot iterate connected DataStreams");
	// }

	protected void addConnection(DataStream<OUT> stream) {
		connectedStreams.add(stream.copy());
	}

	@Override
	protected DataStream<OUT> setConnectionType(StreamPartitioner<OUT> partitioner) {
		ConnectedDataStream<OUT> returnStream = (ConnectedDataStream<OUT>) this.copy();

		for (DataStream<OUT> stream : returnStream.connectedStreams) {
			stream.partitioner = partitioner;
		}

		return returnStream;
	}

	@Override
	protected ConnectedDataStream<OUT> copy() {
		return new ConnectedDataStream<OUT>(this);
	}

}
