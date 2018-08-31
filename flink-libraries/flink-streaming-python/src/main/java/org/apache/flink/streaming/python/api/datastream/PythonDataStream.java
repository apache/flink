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

package org.apache.flink.streaming.python.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.python.api.functions.PyKey;
import org.apache.flink.streaming.python.api.functions.PythonFilterFunction;
import org.apache.flink.streaming.python.api.functions.PythonFlatMapFunction;
import org.apache.flink.streaming.python.api.functions.PythonKeySelector;
import org.apache.flink.streaming.python.api.functions.PythonMapFunction;
import org.apache.flink.streaming.python.api.functions.PythonOutputSelector;
import org.apache.flink.streaming.python.api.functions.PythonSinkFunction;
import org.apache.flink.streaming.python.util.serialization.PythonSerializationSchema;

import org.python.core.PyObject;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A {@code PythonDataStream} is a thin wrapper layer over {@link DataStream}, which represents a
 * stream of elements of the same type. A {@code PythonDataStream} can be transformed into
 * another {@code PythonDataStream} by applying various transformation functions, such as
 * <ul>
 * <li>{@link PythonDataStream#map}
 * <li>{@link PythonDataStream#split}
 * </ul>
 *
 * <p>A thin wrapper layer means that the functionality itself is performed by the
 * {@link DataStream}, however instead of working directly with the streaming data sets,
 * this layer handles Python wrappers (e.g. {@code PythonDataStream}) to comply with the
 * Python standard coding styles.</p>
 */
@PublicEvolving
public class PythonDataStream<D extends DataStream<PyObject>> {
	protected final D stream;

	public PythonDataStream(D stream) {
		this.stream = stream;
	}

	/**
	 * A thin wrapper layer over {@link DataStream#union(DataStream[])}.
	 *
	 * @param streams The Python DataStreams to union output with.
	 * @return The {@link PythonDataStream}.
	 */
	@SafeVarargs
	@SuppressWarnings("unchecked")
	public final PythonDataStream union(PythonDataStream... streams) {
		ArrayList<DataStream<PyObject>> dsList = new ArrayList<>();
		for (PythonDataStream ps : streams) {
			dsList.add(ps.stream);
		}
		DataStream<PyObject>[] dsArray = new DataStream[dsList.size()];
		return new PythonDataStream(stream.union(dsList.toArray(dsArray)));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#split(OutputSelector)}.
	 *
	 * @param output_selector The user defined {@link OutputSelector} for directing the tuples.
	 * @return The {@link PythonSplitStream}
	 */
	public PythonSplitStream split(OutputSelector<PyObject> output_selector) throws IOException {
		return new PythonSplitStream(this.stream.split(new PythonOutputSelector(output_selector)));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#filter(FilterFunction)}.
	 *
	 * @param filter The FilterFunction that is called for each element of the DataStream.
	 * @return The filtered {@link PythonDataStream}.
	 */
	public PythonSingleOutputStreamOperator filter(FilterFunction<PyObject> filter) throws IOException {
		return new PythonSingleOutputStreamOperator(stream.filter(new PythonFilterFunction(filter)));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#map(MapFunction)}.
	 *
	 * @param mapper The MapFunction that is called for each element of the
	 * DataStream.
	 * @return The transformed {@link PythonDataStream}.
	 */
	public PythonDataStream<SingleOutputStreamOperator<PyObject>> map(
		MapFunction<PyObject, PyObject> mapper) throws IOException {
		return new PythonSingleOutputStreamOperator(stream.map(new PythonMapFunction(mapper)));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#flatMap(FlatMapFunction)}.
	 *
	 * @param flat_mapper The FlatMapFunction that is called for each element of the
	 * DataStream
	 * @return The transformed {@link PythonDataStream}.
	 */
	public PythonDataStream<SingleOutputStreamOperator<PyObject>> flat_map(
		FlatMapFunction<PyObject, Object> flat_mapper) throws IOException {
		return new PythonSingleOutputStreamOperator(stream.flatMap(new PythonFlatMapFunction(flat_mapper)));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#keyBy(KeySelector)}.
	 *
	 * @param selector The KeySelector to be used for extracting the key for partitioning
	 * @return The {@link PythonDataStream} with partitioned state (i.e. {@link PythonKeyedStream})
	 */
	public PythonKeyedStream key_by(KeySelector<PyObject, PyKey> selector) throws IOException {
		return new PythonKeyedStream(stream.keyBy(new PythonKeySelector(selector)));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#print()}.
	 */
	@PublicEvolving
	public void output() {
		stream.print();
	}

	/**
	 * A thin wrapper layer over {@link DataStream#writeAsText(java.lang.String)}.
	 *
	 * @param path The path pointing to the location the text file is written to.
	 */
	@PublicEvolving
	public void write_as_text(String path) {
		stream.writeAsText(path);
	}

	/**
	 * A thin wrapper layer over {@link DataStream#writeAsText(java.lang.String, WriteMode)}.
	 *
	 * @param path The path pointing to the location the text file is written to
	 * @param mode Controls the behavior for existing files. Options are
	 * NO_OVERWRITE and OVERWRITE.
	 */
	@PublicEvolving
	public void write_as_text(String path, WriteMode mode) {
		stream.writeAsText(path, mode);
	}

	/**
	 * A thin wrapper layer over {@link DataStream#writeToSocket(String, int, org.apache.flink.api.common.serialization.SerializationSchema)}.
	 *
	 * @param host host of the socket
	 * @param port port of the socket
	 * @param schema schema for serialization
	 */
	@PublicEvolving
	public void write_to_socket(String host, Integer port, SerializationSchema<PyObject> schema) throws IOException {
		stream.writeToSocket(host, port, new PythonSerializationSchema(schema));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#addSink(SinkFunction)}.
	 *
	 * @param sink_func The object containing the sink's invoke function.
	 */
	@PublicEvolving
	public void add_sink(SinkFunction<PyObject> sink_func) throws IOException {
		stream.addSink(new PythonSinkFunction(sink_func));
	}

	/**
	 * A thin wrapper layer over {@link DataStream#iterate()}.
	 *
	 * <p>Initiates an iterative part of the program that feeds back data streams.
	 * The iterative part needs to be closed by calling
	 * {@link PythonIterativeStream#close_with(PythonDataStream)}. The transformation of
	 * this IterativeStream will be the iteration head. The data stream
	 * given to the {@link PythonIterativeStream#close_with(PythonDataStream)} method is
	 * the data stream that will be fed back and used as the input for the
	 * iteration head. </p>
	 *
	 * <p>A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link #split(OutputSelector)} for more information.
	 *
	 * <p>The iteration edge will be partitioned the same way as the first input of
	 * the iteration head unless it is changed in the
	 * {@link PythonIterativeStream#close_with(PythonDataStream)} call.
	 *
	 * <p>By default a PythonDataStream with iteration will never terminate, but the user
	 * can use the maxWaitTime parameter to set a max waiting time for the
	 * iteration head. If no data received in the set time, the stream
	 * terminates.
	 *
	 * @return The iterative data stream created.
	 */
	@PublicEvolving
	public PythonIterativeStream iterate() {
		return new PythonIterativeStream(this.stream.iterate());
	}

	/**
	 * A thin wrapper layer over {@link DataStream#iterate(long)}.
	 *
	 * <p>Initiates an iterative part of the program that feeds back data streams.
	 * The iterative part needs to be closed by calling
	 * {@link PythonIterativeStream#close_with(PythonDataStream)}. The transformation of
	 * this IterativeStream will be the iteration head. The data stream
	 * given to the {@link PythonIterativeStream#close_with(PythonDataStream)} method is
	 * the data stream that will be fed back and used as the input for the
	 * iteration head.</p>
	 *
	 * <p>A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link #split(OutputSelector)} for more information.
	 *
	 * <p>The iteration edge will be partitioned the same way as the first input of
	 * the iteration head unless it is changed in the
	 * {@link PythonIterativeStream#close_with(PythonDataStream)} call.
	 *
	 * <p>By default a PythonDataStream with iteration will never terminate, but the user
	 * can use the maxWaitTime parameter to set a max waiting time for the
	 * iteration head. If no data received in the set time, the stream
	 * terminates.
	 *
	 * @param max_wait_time_ms Number of milliseconds to wait between inputs before shutting
	 * down
	 * @return The iterative data stream created.
	 */
	@PublicEvolving
	public PythonIterativeStream iterate(Long max_wait_time_ms) {
		return new PythonIterativeStream(this.stream.iterate(max_wait_time_ms));
	}
}
