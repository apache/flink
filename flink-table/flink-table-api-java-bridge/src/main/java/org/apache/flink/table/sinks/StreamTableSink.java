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

package org.apache.flink.table.sinks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

/**
 * Defines an external stream table and provides write access to its data.
 *
 * @param <T> Type of the {@link DataStream} created by this {@link TableSink}.
 */
public interface StreamTableSink<T> extends TableSink<T> {

	/**
	 * Emits the DataStream.
	 *
	 * @deprecated This method will be removed in future versions as it returns nothing.
	 *  It is recommended to use {@link #consumeDataStream(DataStream)} instead which
	 *  returns the {@link DataStreamSink}. The returned {@link DataStreamSink} will be
	 *  used to set resources for the sink operator. If the {@link #consumeDataStream(DataStream)}
	 *  is implemented, this method can be empty implementation.
	 */
	@Deprecated
	void emitDataStream(DataStream<T> dataStream);

	/**
	 * Consumes the DataStream and return the sink transformation {@link DataStreamSink}.
	 * The returned {@link DataStreamSink} will be used to set resources for the sink operator.
	 */
	default DataStreamSink<?> consumeDataStream(DataStream<T> dataStream) {
		emitDataStream(dataStream);
		return null;
	}
}
