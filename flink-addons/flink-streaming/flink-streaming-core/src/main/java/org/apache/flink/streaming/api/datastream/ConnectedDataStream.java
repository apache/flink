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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.function.co.CoWindowFunction;
import org.apache.flink.streaming.api.function.co.RichCoMapFunction;
import org.apache.flink.streaming.api.function.co.RichCoReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoFlatMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoWindowInvokable;
import org.apache.flink.streaming.api.windowing.helper.SystemTimestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

/**
 * The ConnectedDataStream represents a stream for two different data types. It
 * can be used to apply transformations like {@link CoMapFunction} on two
 * {@link DataStream}s
 * 
 * @param <IN1>
 *            Type of the first input data steam.
 * @param <IN2>
 *            Type of the second input data stream.
 */
public class ConnectedDataStream<IN1, IN2> {

	protected StreamExecutionEnvironment environment;
	protected JobGraphBuilder jobGraphBuilder;
	protected DataStream<IN1> dataStream1;
	protected DataStream<IN2> dataStream2;

	protected boolean isGrouped;
	protected KeySelector<IN1, ?> keySelector1;
	protected KeySelector<IN2, ?> keySelector2;

	protected ConnectedDataStream(DataStream<IN1> input1, DataStream<IN2> input2) {
		this.jobGraphBuilder = input1.jobGraphBuilder;
		this.environment = input1.environment;
		this.dataStream1 = input1.copy();
		this.dataStream2 = input2.copy();

		if ((input1 instanceof GroupedDataStream) && (input2 instanceof GroupedDataStream)) {
			this.isGrouped = true;
			this.keySelector1 = ((GroupedDataStream<IN1>) input1).keySelector;
			this.keySelector2 = ((GroupedDataStream<IN2>) input2).keySelector;
		} else {
			this.isGrouped = false;
			this.keySelector1 = null;
			this.keySelector2 = null;
		}
	}

	protected ConnectedDataStream(ConnectedDataStream<IN1, IN2> coDataStream) {
		this.jobGraphBuilder = coDataStream.jobGraphBuilder;
		this.environment = coDataStream.environment;
		this.dataStream1 = coDataStream.getFirst();
		this.dataStream2 = coDataStream.getSecond();
		this.isGrouped = coDataStream.isGrouped;
		this.keySelector1 = coDataStream.keySelector1;
		this.keySelector2 = coDataStream.keySelector2;
	}

	public <F> F clean(F f) {
		if (getExecutionEnvironment().getConfig().isClosureCleanerEnabled()) {
			ClosureCleaner.clean(f, true);
		}
		ClosureCleaner.ensureSerializable(f);
		return f;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	/**
	 * Returns the first {@link DataStream}.
	 * 
	 * @return The first DataStream.
	 */
	public DataStream<IN1> getFirst() {
		return dataStream1.copy();
	}

	/**
	 * Returns the second {@link DataStream}.
	 * 
	 * @return The second DataStream.
	 */
	public DataStream<IN2> getSecond() {
		return dataStream2.copy();
	}

	/**
	 * Gets the type of the first input
	 * 
	 * @return The type of the first input
	 */
	public TypeInformation<IN1> getInputType1() {
		return dataStream1.getType();
	}

	/**
	 * Gets the type of the second input
	 * 
	 * @return The type of the second input
	 */
	public TypeInformation<IN2> getInputType2() {
		return dataStream2.getType();
	}

	/**
	 * GroupBy operation for connected data stream. Groups the elements of
	 * input1 and input2 according to keyPosition1 and keyPosition2. Used for
	 * applying function on grouped data streams for example
	 * {@link ConnectedDataStream#reduce}
	 * 
	 * @param keyPosition1
	 *            The field used to compute the hashcode of the elements in the
	 *            first input stream.
	 * @param keyPosition2
	 *            The field used to compute the hashcode of the elements in the
	 *            second input stream.
	 * @return @return The transformed {@link ConnectedDataStream}
	 */
	public ConnectedDataStream<IN1, IN2> groupBy(int keyPosition1, int keyPosition2) {
		return new ConnectedDataStream<IN1, IN2>(dataStream1.groupBy(keyPosition1),
				dataStream2.groupBy(keyPosition2));
	}

	/**
	 * GroupBy operation for connected data stream. Groups the elements of
	 * input1 and input2 according to keyPositions1 and keyPositions2. Used for
	 * applying function on grouped data streams for example
	 * {@link ConnectedDataStream#reduce}
	 * 
	 * @param keyPositions1
	 *            The fields used to group the first input stream.
	 * @param keyPositions2
	 *            The fields used to group the second input stream.
	 * @return @return The transformed {@link ConnectedDataStream}
	 */
	public ConnectedDataStream<IN1, IN2> groupBy(int[] keyPositions1, int[] keyPositions2) {
		return new ConnectedDataStream<IN1, IN2>(dataStream1.groupBy(keyPositions1),
				dataStream2.groupBy(keyPositions2));
	}

	/**
	 * GroupBy operation for connected data stream using key expressions. Groups
	 * the elements of input1 and input2 according to field1 and field2. A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field1
	 *            The grouping expression for the first input
	 * @param field2
	 *            The grouping expression for the second input
	 * @return The grouped {@link ConnectedDataStream}
	 */
	public ConnectedDataStream<IN1, IN2> groupBy(String field1, String field2) {
		return new ConnectedDataStream<IN1, IN2>(dataStream1.groupBy(field1),
				dataStream2.groupBy(field2));
	}

	/**
	 * GroupBy operation for connected data stream using key expressions. Groups
	 * the elements of input1 and input2 according to fields1 and fields2. A
	 * field expression is either the name of a public field or a getter method
	 * with parentheses of the {@link DataStream}S underlying type. A dot can be
	 * used to drill down into objects, as in {@code "field1.getInnerField2()" }
	 * .
	 * 
	 * @param fields1
	 *            The grouping expressions for the first input
	 * @param fields2
	 *            The grouping expressions for the second input
	 * @return The grouped {@link ConnectedDataStream}
	 */
	public ConnectedDataStream<IN1, IN2> groupBy(String[] fields1, String[] fields2) {
		return new ConnectedDataStream<IN1, IN2>(dataStream1.groupBy(fields1),
				dataStream2.groupBy(fields2));
	}

	/**
	 * GroupBy operation for connected data stream. Groups the elements of
	 * input1 and input2 using keySelector1 and keySelector2. Used for applying
	 * function on grouped data streams for example
	 * {@link ConnectedDataStream#reduce}
	 * 
	 * @param keySelector1
	 *            The {@link KeySelector} used for grouping the first input
	 * @param keySelector2
	 *            The {@link KeySelector} used for grouping the second input
	 * @return @return The transformed {@link ConnectedDataStream}
	 */
	public ConnectedDataStream<IN1, IN2> groupBy(KeySelector<IN1, ?> keySelector1,
			KeySelector<IN2, ?> keySelector2) {
		return new ConnectedDataStream<IN1, IN2>(dataStream1.groupBy(keySelector1),
				dataStream2.groupBy(keySelector2));
	}

	/**
	 * Batch operation for connected data stream. Collects each input data
	 * stream's elements simultaneously into sliding batches creating a new
	 * {@link CoBatchedDataStream}. Then the user can apply
	 * {@link CoBatchedDataStream#reduce} transformation on the
	 * {@link CoBatchedDataStream}.
	 * 
	 * @param batchSize1
	 *            The number of elements in each batch of the first input data
	 *            stream
	 * @param batchSize2
	 *            The number of elements in each batch of the second input data
	 *            stream
	 * @param slideSize1
	 *            The number of elements with which the batches of the first
	 *            input data stream are slid by after each transformation.
	 * @param slideSize2
	 *            The number of elements with which the batches of the second
	 *            input data stream are slid by after each transformation.
	 * @return The transformed {@link ConnectedDataStream}
	 */
	public CoBatchedDataStream<IN1, IN2> batch(long batchSize1, long batchSize2, long slideSize1,
			long slideSize2) {
		if (batchSize1 < 1 || batchSize2 < 1) {
			throw new IllegalArgumentException("Batch size must be positive");
		}
		if (slideSize1 < 1 || slideSize2 < 1) {
			throw new IllegalArgumentException("Slide size must be positive");
		}
		return new CoBatchedDataStream<IN1, IN2>(this, batchSize1, batchSize2, slideSize1,
				slideSize2);
	}

	/**
	 * Batch operation for connected data stream. Collects each input data
	 * stream's elements simultaneously into batches creating a new
	 * {@link CoBatchedDataStream}. Then the user can apply
	 * {@link CoBatchedDataStream#reduce} transformation on the
	 * {@link CoBatchedDataStream}.
	 * 
	 * @param batchSize1
	 *            The number of elements in each batch of the first input data
	 *            stream
	 * @param batchSize2
	 *            The number of elements in each batch of the second input data
	 *            stream
	 * @return The transformed {@link ConnectedDataStream}
	 */
	public CoBatchedDataStream<IN1, IN2> batch(long batchSize1, long batchSize2) {
		return batch(batchSize1, batchSize2, batchSize1, batchSize2);
	}

	/**
	 * Window operation for connected data stream. Collects each input data
	 * stream's elements simultaneously into sliding windows creating a new
	 * {@link CoWindowDataStream}. Then the user can apply
	 * {@link WindowDataStream#reduce} transformation on the
	 * {@link CoWindowDataStream}. The user can implement their own time stamps
	 * or use the system time by default.
	 * 
	 * @param windowSize1
	 *            The length of the window of the first input data stream
	 * @param windowSize2
	 *            The length of the window of the second input data stream
	 * @param slideInterval1
	 *            The number of milliseconds with which the windows of the first
	 *            input data stream are slid by after each transformation
	 * @param slideInterval2
	 *            The number of milliseconds with which the windows of the
	 *            second input data stream are slid by after each transformation
	 * @param timeStamp1
	 *            User defined function for extracting time-stamps from each
	 *            element of the first input data stream
	 * @param timeStamp2
	 *            User defined function for extracting time-stamps from each
	 *            element of the second input data stream
	 * @return The transformed {@link ConnectedDataStream}
	 */
	public CoWindowDataStream<IN1, IN2> window(long windowSize1, long windowSize2,
			long slideInterval1, long slideInterval2, TimestampWrapper<IN1> timeStamp1,
			TimestampWrapper<IN2> timeStamp2) {
		if (windowSize1 < 1 || windowSize2 < 1) {
			throw new IllegalArgumentException("Window size must be positive");
		}
		if (slideInterval1 < 1 || slideInterval2 < 1) {
			throw new IllegalArgumentException("Slide interval must be positive");
		}
		return new CoWindowDataStream<IN1, IN2>(this, windowSize1, windowSize2, slideInterval1,
				slideInterval2, timeStamp1, timeStamp2);
	}

	/**
	 * Window operation for connected data stream. Collects each input data
	 * stream's elements simultaneously into sliding windows creating a new
	 * {@link CoWindowDataStream}. Then the user can apply
	 * {@link WindowDataStream#reduce} transformation on the
	 * {@link CoWindowDataStream}.
	 * 
	 * @param windowSize1
	 *            The length of the window of the first input data stream in
	 *            milliseconds.
	 * @param windowSize2
	 *            The length of the window of the second input data stream in
	 *            milliseconds.
	 * @param slideInterval1
	 *            The number of milliseconds with which the windows of the first
	 *            input data stream are slid by after each transformation
	 * @param slideInterval2
	 *            The number of milliseconds with which the windows of the
	 *            second input data stream are slid by after each transformation
	 * @return The transformed {@link ConnectedDataStream}
	 */
	@SuppressWarnings("unchecked")
	public CoWindowDataStream<IN1, IN2> window(long windowSize1, long windowSize2,
			long slideInterval1, long slideInterval2) {
		return window(windowSize1, windowSize2, slideInterval1, slideInterval2,
				(TimestampWrapper<IN1>) SystemTimestamp.getWrapper(),
				(TimestampWrapper<IN2>) SystemTimestamp.getWrapper());
	}

	/**
	 * Window operation for connected data stream. Collects each input data
	 * stream's elements simultaneously into windows creating a new
	 * {@link CoWindowDataStream}. Then the user can apply
	 * {@link WindowDataStream#reduce} transformation on the
	 * {@link CoWindowDataStream}. The user can implement their own time stamps
	 * or use the system time by default.
	 * 
	 * @param windowSize1
	 *            The length of the window of the first input data stream
	 * @param windowSize2
	 *            The length of the window of the second input data stream
	 * @param timeStamp1
	 *            User defined function for extracting time-stamps from each
	 *            element of the first input data stream
	 * @param timeStamp2
	 *            User defined function for extracting time-stamps from each
	 *            element of the second input data stream
	 * @return The transformed {@link ConnectedDataStream}
	 */
	public CoWindowDataStream<IN1, IN2> window(long windowSize1, long windowSize2,
			TimestampWrapper<IN1> timeStamp1, TimestampWrapper<IN2> timeStamp2) {
		return window(windowSize1, windowSize2, windowSize1, windowSize2, timeStamp1, timeStamp2);
	}

	/**
	 * Window operation for connected data stream. Collects each input data
	 * stream's elements simultaneously into windows creating a new
	 * {@link CoWindowDataStream}. Then the user can apply
	 * {@link WindowDataStream#reduce} transformation on the
	 * {@link CoWindowDataStream}.
	 * 
	 * @param windowSize1
	 *            The length of the window of the first input data stream in
	 *            milliseconds
	 * @param windowSize2
	 *            The length of the window of the second input data stream in
	 *            milliseconds
	 * @return The transformed {@link ConnectedDataStream}
	 */
	@SuppressWarnings("unchecked")
	public CoWindowDataStream<IN1, IN2> window(long windowSize1, long windowSize2) {
		return window(windowSize1, windowSize2, windowSize1, windowSize2,
				(TimestampWrapper<IN1>) SystemTimestamp.getWrapper(),
				(TimestampWrapper<IN2>) SystemTimestamp.getWrapper());
	}

	/**
	 * Applies a CoMap transformation on a {@link ConnectedDataStream} and maps
	 * the output to a common type. The transformation calls a
	 * {@link CoMapFunction#map1} for each element of the first input and
	 * {@link CoMapFunction#map2} for each element of the second input. Each
	 * CoMapFunction call returns exactly one element. The user can also extend
	 * {@link RichCoMapFunction} to gain access to other features provided by
	 * the {@link RichFuntion} interface.
	 * 
	 * @param coMapper
	 *            The CoMapFunction used to jointly transform the two input
	 *            DataStreams
	 * @return The transformed {@link DataStream}
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> map(CoMapFunction<IN1, IN2, OUT> coMapper) {
		TypeInformation<OUT> outTypeInfo = TypeExtractor.createTypeInfo(CoMapFunction.class,
				coMapper.getClass(), 2, null, null);

		return addCoFunction("coMap", outTypeInfo, new CoMapInvokable<IN1, IN2, OUT>(
				clean(coMapper)));

	}

	/**
	 * Applies a CoFlatMap transformation on a {@link ConnectedDataStream} and
	 * maps the output to a common type. The transformation calls a
	 * {@link CoFlatMapFunction#flatMap1} for each element of the first input
	 * and {@link CoFlatMapFunction#flatMap2} for each element of the second
	 * input. Each CoFlatMapFunction call returns any number of elements
	 * including none. The user can also extend {@link RichFlatMapFunction} to
	 * gain access to other features provided by the {@link RichFuntion}
	 * interface.
	 * 
	 * @param coFlatMapper
	 *            The CoFlatMapFunction used to jointly transform the two input
	 *            DataStreams
	 * @return The transformed {@link DataStream}
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> flatMap(
			CoFlatMapFunction<IN1, IN2, OUT> coFlatMapper) {
		TypeInformation<OUT> outTypeInfo = TypeExtractor.createTypeInfo(CoFlatMapFunction.class,
				coFlatMapper.getClass(), 2, null, null);

		return addCoFunction("coFlatMap", outTypeInfo, new CoFlatMapInvokable<IN1, IN2, OUT>(
				clean(coFlatMapper)));
	}

	/**
	 * Applies a reduce transformation on a {@link ConnectedDataStream} and maps
	 * the outputs to a common type. If the {@link ConnectedDataStream} is
	 * batched or windowed then the reduce transformation is applied on every
	 * sliding batch/window of the data stream. If the connected data stream is
	 * grouped then the reducer is applied on every group of elements sharing
	 * the same key. This type of reduce is much faster than reduceGroup since
	 * the reduce function can be applied incrementally. The user can also
	 * extend the {@link RichCoReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coReducer
	 *            The {@link CoReduceFunction} that will be called for every
	 *            element of the inputs.
	 * @return The transformed {@link DataStream}.
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> reduce(CoReduceFunction<IN1, IN2, OUT> coReducer) {

		TypeInformation<OUT> outTypeInfo = TypeExtractor.createTypeInfo(CoReduceFunction.class,
				coReducer.getClass(), 2, null, null);

		return addCoFunction("coReduce", outTypeInfo, getReduceInvokable(clean(coReducer)));

	}

	/**
	 * Applies a CoWindow transformation on the connected DataStreams. The
	 * transformation calls the {@link CoWindowFunction#coWindow} method for for
	 * time aligned windows of the two data streams. System time is used as
	 * default to compute windows.
	 * 
	 * @param coWindowFunction
	 *            The {@link CoWindowFunction} that will be applied for the time
	 *            windows.
	 * @param windowSize
	 *            Size of the windows that will be aligned for both streams in
	 *            milliseconds.
	 * @param slideInterval
	 *            After every function call the windows will be slid by this
	 *            interval.
	 * 
	 * @return The transformed {@link DataStream}.
	 */
	@SuppressWarnings("unchecked")
	public <OUT> SingleOutputStreamOperator<OUT, ?> windowReduce(
			CoWindowFunction<IN1, IN2, OUT> coWindowFunction, long windowSize, long slideInterval) {
		return windowReduce(coWindowFunction, windowSize, slideInterval,
				(TimestampWrapper<IN1>) SystemTimestamp.getWrapper(),
				(TimestampWrapper<IN2>) SystemTimestamp.getWrapper());
	}

	/**
	 * Applies a CoWindow transformation on the connected DataStreams. The
	 * transformation calls the {@link CoWindowFunction#coWindow} method for
	 * time aligned windows of the two data streams. The user can implement
	 * their own time stamps or use the system time by default.
	 * 
	 * @param coWindowFunction
	 *            The {@link CoWindowFunction} that will be applied for the time
	 *            windows.
	 * @param windowSize
	 *            Size of the windows that will be aligned for both streams. If
	 *            system time is used it is milliseconds. User defined time
	 *            stamps are assumed to be monotonically increasing.
	 * @param slideInterval
	 *            After every function call the windows will be slid by this
	 *            interval.
	 * 
	 * @param timestamp1
	 *            User defined time stamps for the first input.
	 * @param timestamp2
	 *            User defined time stamps for the second input.
	 * @return The transformed {@link DataStream}.
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> windowReduce(
			CoWindowFunction<IN1, IN2, OUT> coWindowFunction, long windowSize, long slideInterval,
			TimestampWrapper<IN1> timestamp1, TimestampWrapper<IN2> timestamp2) {

		if (windowSize < 1) {
			throw new IllegalArgumentException("Window size must be positive");
		}
		if (slideInterval < 1) {
			throw new IllegalArgumentException("Slide interval must be positive");
		}

		TypeInformation<OUT> outTypeInfo = TypeExtractor.createTypeInfo(CoWindowFunction.class,
				coWindowFunction.getClass(), 2, null, null);

		return addCoFunction("coWindowReduce", outTypeInfo, new CoWindowInvokable<IN1, IN2, OUT>(
				clean(coWindowFunction), windowSize, slideInterval, timestamp1, timestamp2));

	}

	protected <OUT> CoInvokable<IN1, IN2, OUT> getReduceInvokable(
			CoReduceFunction<IN1, IN2, OUT> coReducer) {
		CoReduceInvokable<IN1, IN2, OUT> invokable;
		if (isGrouped) {
			invokable = new CoGroupedReduceInvokable<IN1, IN2, OUT>(clean(coReducer), keySelector1,
					keySelector2);
		} else {
			invokable = new CoReduceInvokable<IN1, IN2, OUT>(clean(coReducer));
		}
		return invokable;
	}

	public <OUT> SingleOutputStreamOperator<OUT, ?> addGeneralWindowCombine(
			CoWindowFunction<IN1, IN2, OUT> coWindowFunction, TypeInformation<OUT> outTypeInfo,
			long windowSize, long slideInterval, TimestampWrapper<IN1> timestamp1,
			TimestampWrapper<IN2> timestamp2) {

		if (windowSize < 1) {
			throw new IllegalArgumentException("Window size must be positive");
		}
		if (slideInterval < 1) {
			throw new IllegalArgumentException("Slide interval must be positive");
		}

		return addCoFunction("coWindowReduce", outTypeInfo, new CoWindowInvokable<IN1, IN2, OUT>(
				clean(coWindowFunction), windowSize, slideInterval, timestamp1, timestamp2));

	}

	public <OUT> SingleOutputStreamOperator<OUT, ?> addCoFunction(String functionName,
			TypeInformation<OUT> outTypeInfo, CoInvokable<IN1, IN2, OUT> functionInvokable) {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<OUT, ?> returnStream = new SingleOutputStreamOperator(
				environment, functionName, outTypeInfo);

		dataStream1.jobGraphBuilder.addCoTask(returnStream.getId(), functionInvokable,
				getInputType1(), getInputType2(), outTypeInfo, functionName,
				environment.getDegreeOfParallelism());

		dataStream1.connectGraph(dataStream1, returnStream.getId(), 1);
		dataStream1.connectGraph(dataStream2, returnStream.getId(), 2);

		return returnStream;
	}

	protected ConnectedDataStream<IN1, IN2> copy() {
		return new ConnectedDataStream<IN1, IN2>(this);
	}

}
