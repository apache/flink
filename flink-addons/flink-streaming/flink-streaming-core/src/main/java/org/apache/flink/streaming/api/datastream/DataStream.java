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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.RichFilterFunction;
import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.sink.WriteFormatAsCsv;
import org.apache.flink.streaming.api.function.sink.WriteFormatAsText;
import org.apache.flink.streaming.api.function.sink.WriteSinkFunctionByBatches;
import org.apache.flink.streaming.api.function.sink.WriteSinkFunctionByMillis;
import org.apache.flink.streaming.api.invokable.SinkInvokable;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.invokable.operator.BatchReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.FilterInvokable;
import org.apache.flink.streaming.api.invokable.operator.FlatMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.MapInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowReduceInvokable;
import org.apache.flink.streaming.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.partitioner.DistributePartitioner;
import org.apache.flink.streaming.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.TypeSerializerWrapper;

/**
 * A DataStream represents a stream of elements of the same type. A DataStream
 * can be transformed into another DataStream by applying a transformation as
 * for example
 * <ul>
 * <li>{@link DataStream#map},</li>
 * <li>{@link DataStream#filter}, or</li>
 * <li>{@link DataStream#batchReduce}.</li>
 * </ul>
 * 
 * @param <OUT>
 *            The type of the DataStream, i.e., the type of the elements of the
 *            DataStream.
 */
public abstract class DataStream<OUT> {

	protected static Integer counter = 0;
	protected final StreamExecutionEnvironment environment;
	protected final String id;
	protected int degreeOfParallelism;
	protected List<String> userDefinedNames;
	protected StreamPartitioner<OUT> partitioner;

	protected final JobGraphBuilder jobGraphBuilder;

	/**
	 * Create a new {@link DataStream} in the given execution environment with
	 * partitioning set to forward by default.
	 * 
	 * @param environment
	 *            StreamExecutionEnvironment
	 * @param operatorType
	 *            The type of the operator in the component
	 */
	public DataStream(StreamExecutionEnvironment environment, String operatorType) {
		if (environment == null) {
			throw new NullPointerException("context is null");
		}

		// TODO add name based on component number an preferable sequential id
		counter++;
		this.id = operatorType + "-" + counter.toString();
		this.environment = environment;
		this.degreeOfParallelism = environment.getDegreeOfParallelism();
		this.jobGraphBuilder = environment.getJobGraphBuilder();
		this.userDefinedNames = new ArrayList<String>();
		this.partitioner = new ForwardPartitioner<OUT>();

	}

	/**
	 * Create a new DataStream by creating a copy of another DataStream
	 * 
	 * @param dataStream
	 *            The DataStream that will be copied.
	 */
	public DataStream(DataStream<OUT> dataStream) {
		this.environment = dataStream.environment;
		this.id = dataStream.id;
		this.degreeOfParallelism = dataStream.degreeOfParallelism;
		this.userDefinedNames = new ArrayList<String>(dataStream.userDefinedNames);
		this.partitioner = dataStream.partitioner;
		this.jobGraphBuilder = dataStream.jobGraphBuilder;

	}

	/**
	 * Partitioning strategy on the stream.
	 */
	public static enum ConnectionType {
		SHUFFLE, BROADCAST, FIELD, FORWARD, DISTRIBUTE
	}

	/**
	 * Returns the ID of the {@link DataStream}.
	 * 
	 * @return ID of the DataStream
	 */
	public String getId() {
		return id;
	}

	/**
	 * Gets the degree of parallelism for this operator.
	 * 
	 * @return The parallelism set for this operator.
	 */
	public int getParallelism() {
		return this.degreeOfParallelism;
	}

	/**
	 * Creates a new {@link MergedDataStream} by merging {@link DataStream}
	 * outputs of the same type with each other. The DataStreams merged using
	 * this operator will be transformed simultaneously.
	 * 
	 * @param streams
	 *            The DataStreams to merge output with.
	 * @return The {@link MergedDataStream}.
	 */
	public MergedDataStream<OUT> merge(DataStream<OUT>... streams) {
		MergedDataStream<OUT> returnStream = new MergedDataStream<OUT>(this);

		for (DataStream<OUT> stream : streams) {
			returnStream.addConnection(stream);
		}
		return returnStream;
	}

	/**
	 * Creates a new {@link ConnectedDataStream} by connecting
	 * {@link DataStream} outputs of different type with each other. The
	 * DataStreams connected using this operators can be used with CoFunctions.
	 * 
	 * @param dataStream
	 *            The DataStream with which this stream will be joined.
	 * @return The {@link ConnectedDataStream}.
	 */
	public <R> ConnectedDataStream<OUT, R> connect(DataStream<R> dataStream) {
		return new ConnectedDataStream<OUT, R>(environment, jobGraphBuilder, this, dataStream);
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are partitioned by their hashcode and are sent to only one component.
	 * 
	 * @param keyPosition
	 *            The field used to compute the hashcode.
	 * @return The DataStream with field partitioning set.
	 */
	public DataStream<OUT> partitionBy(int keyPosition) {
		if (keyPosition < 0) {
			throw new IllegalArgumentException("The position of the field must be non-negative");
		}

		return setConnectionType(new FieldsPartitioner<OUT>(keyPosition));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are broadcasted to every parallel instance of the next component.
	 * 
	 * @return The DataStream with broadcast partitioning set.
	 */
	public DataStream<OUT> broadcast() {
		return setConnectionType(new BroadcastPartitioner<OUT>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are shuffled to the next component.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> shuffle() {
		return setConnectionType(new ShufflePartitioner<OUT>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are forwarded to the local subtask of the next component. This is the
	 * default partitioner setting.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> forward() {
		return setConnectionType(new ForwardPartitioner<OUT>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are distributed evenly to the next component.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> distribute() {
		return setConnectionType(new DistributePartitioner<OUT>());
	}

	/**
	 * Applies a Map transformation on a {@link DataStream}. The transformation
	 * calls a {@link MapFunction} for each element of the DataStream. Each
	 * MapFunction call returns exactly one element. The user can also extend
	 * {@link RichMapFunction} to gain access to other features provided by the
	 * {@link RichFuntion} interface.
	 * 
	 * @param mapper
	 *            The MapFunction that is called for each element of the
	 *            DataStream.
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> map(MapFunction<OUT, R> mapper) {
		return addFunction("map", mapper, new FunctionTypeWrapper<OUT, Tuple, R>(mapper,
				MapFunction.class, 0, -1, 1), new MapInvokable<OUT, R>(mapper));
	}

	/**
	 * Applies a FlatMap transformation on a {@link DataStream}. The
	 * transformation calls a {@link FlatMapFunction} for each element of the
	 * DataStream. Each FlatMapFunction call can return any number of elements
	 * including none. The user can also extend {@link RichFlatMapFunction} to
	 * gain access to other features provided by the {@link RichFuntion}
	 * interface.
	 * 
	 * @param flatMapper
	 *            The FlatMapFunction that is called for each element of the
	 *            DataStream
	 * 
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> flatMap(FlatMapFunction<OUT, R> flatMapper) {
		return addFunction("flatMap", flatMapper, new FunctionTypeWrapper<OUT, Tuple, R>(
				flatMapper, FlatMapFunction.class, 0, -1, 1), new FlatMapInvokable<OUT, R>(
				flatMapper));
	}

	public GroupedDataStream<OUT> groupBy(int keyPosition) {
		return new GroupedDataStream<OUT>(this.partitionBy(keyPosition), keyPosition);
	}

	/**
	 * Applies a reduce transformation on preset chunks of the DataStream. The
	 * transformation calls a {@link GroupReduceFunction} for each tuple batch
	 * of the predefined size. Each GroupReduceFunction call can return any
	 * number of elements including none. The user can also extend
	 * {@link RichGroupReduceFunction} to gain access to other features provided
	 * by the {@link RichFuntion} interface.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each tuple batch.
	 * @param batchSize
	 *            The number of tuples grouped together in the batch.
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> batchReduce(GroupReduceFunction<OUT, R> reducer,
			long batchSize) {
		return batchReduce(reducer, batchSize, batchSize);
	}

	/**
	 * Applies a reduce transformation on preset sliding chunks of the
	 * DataStream. The transformation calls a {@link GroupReduceFunction} for
	 * each tuple batch of the predefined size. The tuple batch gets slid by the
	 * given number of tuples. Each GroupReduceFunction call can return any
	 * number of elements including none. The user can also extend
	 * {@link RichGroupReduceFunction} to gain access to other features provided
	 * by the {@link RichFuntion} interface.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each tuple batch.
	 * @param batchSize
	 *            The number of tuples grouped together in the batch.
	 * @param slideSize
	 *            The number of tuples the batch is slid by.
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> batchReduce(GroupReduceFunction<OUT, R> reducer,
			long batchSize, long slideSize) {
		return addFunction("batchReduce", reducer, new FunctionTypeWrapper<OUT, Tuple, R>(reducer,
				GroupReduceFunction.class, 0, -1, 1), new BatchReduceInvokable<OUT, R>(reducer,
				batchSize, slideSize));
	}

	/**
	 * Applies a reduce transformation on preset "time" chunks of the
	 * DataStream. The transformation calls a {@link GroupReduceFunction} on
	 * records received during the predefined time window. The window is shifted
	 * after each reduce call. Each GroupReduceFunction call can return any
	 * number of elements including none.The user can also extend
	 * {@link RichGroupReduceFunction} to gain access to other features provided
	 * by the {@link RichFuntion} interface.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each time window.
	 * @param windowSize
	 *            SingleOutputStreamOperator The time window to run the reducer
	 *            on, in milliseconds.
	 * @param <R>
	 *            output type
	 * @return The transformed DataStream.
	 */
	public <R> SingleOutputStreamOperator<R, ?> windowReduce(GroupReduceFunction<OUT, R> reducer,
			long windowSize) {
		return windowReduce(reducer, windowSize, windowSize);
	}

	/**
	 * Applies a reduce transformation on preset "time" chunks of the
	 * DataStream. The transformation calls a {@link GroupReduceFunction} on
	 * records received during the predefined time window. The window is shifted
	 * after each reduce call. Each GroupReduceFunction call can return any
	 * number of elements including none.The user can also extend
	 * {@link RichGroupReduceFunction} to gain access to other features provided
	 * by the {@link RichFuntion} interface.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each time window.
	 * @param windowSize
	 *            SingleOutputStreamOperator The time window to run the reducer
	 *            on, in milliseconds.
	 * @param slideInterval
	 *            The time interval, batch is slid by.
	 * @param <R>
	 *            output type
	 * @return The transformed DataStream.
	 */
	public <R> SingleOutputStreamOperator<R, ?> windowReduce(GroupReduceFunction<OUT, R> reducer,
			long windowSize, long slideInterval) {
		return addFunction("batchReduce", reducer, new FunctionTypeWrapper<OUT, Tuple, R>(reducer,
				GroupReduceFunction.class, 0, -1, 1), new WindowReduceInvokable<OUT, R>(reducer,
				windowSize, slideInterval));
	}

	/**
	 * Applies a Filter transformation on a {@link DataStream}. The
	 * transformation calls a {@link FilterFunction} for each element of the
	 * DataStream and retains only those element for which the function returns
	 * true. Elements for which the function returns false are filtered. The
	 * user can also extend {@link RichFilterFunction} to gain access to other
	 * features provided by the {@link RichFuntion} interface.
	 * 
	 * @param filter
	 *            The FilterFunction that is called for each element of the
	 *            DataSet.
	 * @return The filtered DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> filter(FilterFunction<OUT> filter) {
		return addFunction("filter", filter, new FunctionTypeWrapper<OUT, Tuple, OUT>(filter,
				FilterFunction.class, 0, -1, 0), new FilterInvokable<OUT>(filter));
	}

	/**
	 * Writes a DataStream to the standard output stream (stdout). For each
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @return The closed DataStream.
	 */
	public DataStreamSink<OUT> print() {
		DataStream<OUT> inputStream = this.copy();
		PrintSinkFunction<OUT> printFunction = new PrintSinkFunction<OUT>();
		DataStreamSink<OUT> returnStream = addSink(inputStream, printFunction, null);

		jobGraphBuilder.setBytesFrom(inputStream.getId(), returnStream.getId());

		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), 1, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, long millis) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), millis, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, int batchSize) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), batchSize, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, long millis, OUT endTuple) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), millis, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, int batchSize, OUT endTuple) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), batchSize, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsText(DataStream<OUT> inputStream, String path,
			WriteFormatAsText<OUT> format, long millis, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream, new WriteSinkFunctionByMillis<OUT>(
				path, format, millis, endTuple), null);
		jobGraphBuilder.setBytesFrom(inputStream.getId(), returnStream.getId());
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsText(DataStream<OUT> inputStream, String path,
			WriteFormatAsText<OUT> format, int batchSize, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream,
				new WriteSinkFunctionByBatches<OUT>(path, format, batchSize, endTuple), null);
		jobGraphBuilder.setBytesFrom(inputStream.getId(), returnStream.getId());
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), 1, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, long millis) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), millis, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, int batchSize) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), batchSize, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, long millis, OUT endTuple) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), millis, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, int batchSize, OUT endTuple) {
		if (this instanceof SingleOutputStreamOperator) {
			((SingleOutputStreamOperator<?, ?>) this).setMutability(false);
		}
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), batchSize, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsCsv(DataStream<OUT> inputStream, String path,
			WriteFormatAsCsv<OUT> format, long millis, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream, new WriteSinkFunctionByMillis<OUT>(
				path, format, millis, endTuple));
		jobGraphBuilder.setBytesFrom(inputStream.getId(), returnStream.getId());
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsCsv(DataStream<OUT> inputStream, String path,
			WriteFormatAsCsv<OUT> format, int batchSize, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream,
				new WriteSinkFunctionByBatches<OUT>(path, format, batchSize, endTuple), null);
		jobGraphBuilder.setBytesFrom(inputStream.getId(), returnStream.getId());
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	/**
	 * Initiates an iterative part of the program that executes multiple times
	 * and feeds back data streams. The iterative part needs to be closed by
	 * calling {@link IterativeDataStream#closeWith(DataStream)}. The
	 * transformation of this IterativeDataStream will be the iteration head.
	 * The data stream given to the {@code closeWith(DataStream)} method is the
	 * data stream that will be fed back and used as the input for the iteration
	 * head. Unlike in batch processing by default the output of the iteration
	 * stream is directed to both to the iteration head and the next component.
	 * To direct tuples to the iteration head or the output specifically one can
	 * use the {@code split(OutputSelector)} on the iteration tail while
	 * referencing the iteration head as 'iterate'.
	 * <p>
	 * The iteration edge will be partitioned the same way as the first input of
	 * the iteration head.
	 * <p>
	 * By default a DataStream with iteration will never terminate, but the user
	 * can use the {@link IterativeDataStream#setMaxWaitTime} call to set a max
	 * waiting time for the iteration.
	 * 
	 * @return The iterative data stream created.
	 */
	public IterativeDataStream<OUT> iterate() {
		return new IterativeDataStream<OUT>(this);
	}

	protected <R> DataStream<OUT> addIterationSource(String iterationID, long waitTime) {

		DataStream<R> returnStream = new DataStreamSource<R>(environment, "iterationSource");

		jobGraphBuilder.addIterationSource(returnStream.getId(), this.getId(), iterationID,
				degreeOfParallelism, waitTime);

		return this.copy();
	}

	/**
	 * Internal function for passing the user defined functions to the JobGraph
	 * of the job.
	 * 
	 * @param functionName
	 *            name of the function
	 * @param function
	 *            the user defined function
	 * @param functionInvokable
	 *            the wrapping JobVertex instance
	 * @param <R>
	 *            type of the return stream
	 * @return the data stream constructed
	 */
	protected <R> SingleOutputStreamOperator<R, ?> addFunction(String functionName,
			final Function function, TypeSerializerWrapper<OUT, Tuple, R> typeWrapper,
			UserTaskInvokable<OUT, R> functionInvokable) {

		DataStream<OUT> inputStream = this.copy();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<R, ?> returnStream = new SingleOutputStreamOperator(environment,
				functionName);

		try {
			jobGraphBuilder.addTask(returnStream.getId(), functionInvokable, typeWrapper,
					functionName, SerializationUtils.serialize((Serializable) function),
					degreeOfParallelism);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize user defined function");
		}

		connectGraph(inputStream, returnStream.getId(), 0);

		if (inputStream instanceof IterativeDataStream) {
			IterativeDataStream<OUT> iterativeStream = (IterativeDataStream<OUT>) inputStream;
			returnStream.addIterationSource(iterativeStream.iterationID.toString(),
					iterativeStream.waitTime);
		}

		return returnStream;
	}

	/**
	 * Internal function for setting the partitioner for the DataStream
	 * 
	 * @param partitioner
	 *            Partitioner to set.
	 * @return The modified DataStream.
	 */
	protected DataStream<OUT> setConnectionType(StreamPartitioner<OUT> partitioner) {
		DataStream<OUT> returnStream = this.copy();

		returnStream.partitioner = partitioner;

		return returnStream;
	}

	/**
	 * Internal function for assembling the underlying
	 * {@link org.apache.flink.nephele.jobgraph.JobGraph} of the job. Connects
	 * the outputs of the given input stream to the specified output stream
	 * given by the outputID.
	 * 
	 * @param inputStream
	 *            input data stream
	 * @param outputID
	 *            ID of the output
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 */
	protected <X> void connectGraph(DataStream<X> inputStream, String outputID, int typeNumber) {
		if (inputStream instanceof MergedDataStream) {
			for (DataStream<X> stream : ((MergedDataStream<X>) inputStream).mergedStreams) {
				jobGraphBuilder.setEdge(stream.getId(), outputID, stream.partitioner, typeNumber,
						inputStream.userDefinedNames);
			}
		} else {
			jobGraphBuilder.setEdge(inputStream.getId(), outputID, inputStream.partitioner,
					typeNumber, inputStream.userDefinedNames);
		}

	}

	/**
	 * Adds the given sink to this DataStream. Only streams with sinks added
	 * will be executed once the {@link StreamExecutionEnvironment#execute()}
	 * method is called.
	 * 
	 * @param sinkFunction
	 *            The object containing the sink's invoke function.
	 * @return The closed DataStream.
	 */
	public DataStreamSink<OUT> addSink(SinkFunction<OUT> sinkFunction) {
		return addSink(this.copy(), sinkFunction);
	}

	private DataStreamSink<OUT> addSink(DataStream<OUT> inputStream, SinkFunction<OUT> sinkFunction) {
		return addSink(inputStream, sinkFunction, new FunctionTypeWrapper<OUT, Tuple, OUT>(
				sinkFunction, SinkFunction.class, 0, -1, 0));
	}

	private DataStreamSink<OUT> addSink(DataStream<OUT> inputStream,
			SinkFunction<OUT> sinkFunction, TypeSerializerWrapper<OUT, Tuple, OUT> typeWrapper) {
		DataStreamSink<OUT> returnStream = new DataStreamSink<OUT>(environment, "sink");

		try {
			jobGraphBuilder.addSink(returnStream.getId(), new SinkInvokable<OUT>(sinkFunction),
					typeWrapper, "sink", SerializationUtils.serialize(sinkFunction),
					degreeOfParallelism);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize SinkFunction");
		}

		inputStream.connectGraph(inputStream.copy(), returnStream.getId(), 0);

		return returnStream;
	}

	/**
	 * Creates a copy of the {@link DataStream}
	 * 
	 * @return The copy
	 */
	protected abstract DataStream<OUT> copy();
}