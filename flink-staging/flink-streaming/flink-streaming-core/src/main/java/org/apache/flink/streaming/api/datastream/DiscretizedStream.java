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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.windowing.WindowFlattener;
import org.apache.flink.streaming.api.invokable.operator.windowing.WindowFolder;
import org.apache.flink.streaming.api.invokable.operator.windowing.WindowMapper;
import org.apache.flink.streaming.api.invokable.operator.windowing.WindowMerger;
import org.apache.flink.streaming.api.invokable.operator.windowing.WindowPartitioner;
import org.apache.flink.streaming.api.invokable.operator.windowing.WindowReducer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.StreamWindowTypeInfo;
import org.apache.flink.streaming.api.windowing.WindowUtils.WindowKey;
import org.apache.flink.streaming.api.windowing.WindowUtils.WindowTransformation;

/**
 * A {@link DiscretizedStream} represents a data stream that has been divided
 * into windows (predefined chunks). User defined function such as
 * {@link #reduceWindow(ReduceFunction)}, {@link #mapWindow()},
 * {@link #foldWindow(FoldFunction, initialValue)} or aggregations can be
 * applied to the windows.
 * 
 * @param <OUT>
 *            The output type of the {@link DiscretizedStream}
 */
public class DiscretizedStream<OUT> extends WindowedDataStream<OUT> {

	private SingleOutputStreamOperator<StreamWindow<OUT>, ?> discretizedStream;
	private WindowTransformation transformation;
	protected boolean isPartitioned = false;

	protected DiscretizedStream(SingleOutputStreamOperator<StreamWindow<OUT>, ?> discretizedStream,
			KeySelector<OUT, ?> groupByKey, WindowTransformation tranformation,
			boolean isPartitioned) {
		super();
		this.groupByKey = groupByKey;
		this.discretizedStream = discretizedStream;
		this.transformation = tranformation;
		this.isPartitioned = isPartitioned;
	}

	public DataStream<OUT> flatten() {
		return discretizedStream.transform("Window Flatten", getType(), new WindowFlattener<OUT>());
	}

	public DataStream<StreamWindow<OUT>> getDiscretizedStream() {
		return discretizedStream;
	}

	@Override
	public DiscretizedStream<OUT> reduceWindow(ReduceFunction<OUT> reduceFunction) {

		DiscretizedStream<OUT> out = partition(transformation).transform(
				WindowTransformation.REDUCEWINDOW, "Window Reduce", getType(),
				new WindowReducer<OUT>(reduceFunction)).merge();

		// If we merged a non-grouped reduce transformation we need to reduce
		// again
		if (!isGrouped() && out.discretizedStream.invokable instanceof WindowMerger) {
			return out.transform(WindowTransformation.REDUCEWINDOW, "Window Reduce", out.getType(),
					new WindowReducer<OUT>(discretizedStream.clean(reduceFunction)));
		} else {
			return out;
		}
	}

	@Override
	public <R> DiscretizedStream<R> mapWindow(WindowMapFunction<OUT, R> windowMapFunction) {

		TypeInformation<R> retType = getWindowMapReturnTypes(windowMapFunction, getType());

		return mapWindow(windowMapFunction, retType);
	}

	@Override
	public <R> DiscretizedStream<R> mapWindow(WindowMapFunction<OUT, R> windowMapFunction,
			TypeInformation<R> returnType) {
		DiscretizedStream<R> out = partition(transformation).transform(
				WindowTransformation.MAPWINDOW, "Window Map", returnType,
				new WindowMapper<OUT, R>(discretizedStream.clean(windowMapFunction))).merge();

		return out;
	}

	@Override
	public <R> DiscretizedStream<R> foldWindow(R initialValue, FoldFunction<OUT, R> foldFunction,
			TypeInformation<R> outType) {

		DiscretizedStream<R> out = partition(transformation).transform(
				WindowTransformation.FOLDWINDOW, "Fold Window", outType,
				new WindowFolder<OUT, R>(discretizedStream.clean(foldFunction), initialValue))
				.merge();
		return out;
	}

	private <R> DiscretizedStream<R> transform(WindowTransformation transformation,
			String operatorName, TypeInformation<R> retType,
			StreamInvokable<StreamWindow<OUT>, StreamWindow<R>> invokable) {

		return wrap(discretizedStream.transform(operatorName, new StreamWindowTypeInfo<R>(retType),
				invokable), transformation);
	}

	private DiscretizedStream<OUT> partition(WindowTransformation transformation) {

		int parallelism = discretizedStream.getParallelism();

		if (isGrouped()) {
			DiscretizedStream<OUT> out = transform(transformation, "Window partitioner", getType(),
					new WindowPartitioner<OUT>(groupByKey)).setParallelism(parallelism);

			out.groupByKey = null;
			out.isPartitioned = true;

			return out;
		} else if (transformation == WindowTransformation.REDUCEWINDOW
				&& parallelism != discretizedStream.getExecutionEnvironment()
						.getDegreeOfParallelism()) {
			DiscretizedStream<OUT> out = transform(transformation, "Window partitioner", getType(),
					new WindowPartitioner<OUT>(parallelism)).setParallelism(parallelism);

			out.isPartitioned = true;

			return out;
		} else {
			this.isPartitioned = false;
			return this;
		}
	}

	private DiscretizedStream<OUT> setParallelism(int parallelism) {
		return wrap(discretizedStream.setParallelism(parallelism), isPartitioned);
	}

	private DiscretizedStream<OUT> merge() {
		TypeInformation<StreamWindow<OUT>> type = discretizedStream.getType();

		// Only merge partitioned streams
		if (isPartitioned) {
			return wrap(
					discretizedStream.groupBy(new WindowKey<OUT>()).transform("Window Merger",
							type, new WindowMerger<OUT>()), false);
		} else {
			return this;
		}

	}

	@SuppressWarnings("rawtypes")
	private <R> DiscretizedStream<R> wrap(SingleOutputStreamOperator stream, boolean isPartitioned) {
		return wrap(stream, transformation);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <R> DiscretizedStream<R> wrap(SingleOutputStreamOperator stream,
			WindowTransformation transformation) {
		return new DiscretizedStream<R>(stream, (KeySelector<R, ?>) this.groupByKey,
				transformation, isPartitioned);
	}

	@SuppressWarnings("rawtypes")
	protected Class<?> getClassAtPos(int pos) {
		Class<?> type;
		TypeInformation<OUT> outTypeInfo = getType();
		if (outTypeInfo.isTupleType()) {
			type = ((TupleTypeInfo) outTypeInfo).getTypeAt(pos).getTypeClass();

		} else if (outTypeInfo instanceof BasicArrayTypeInfo) {

			type = ((BasicArrayTypeInfo) outTypeInfo).getComponentTypeClass();

		} else if (outTypeInfo instanceof PrimitiveArrayTypeInfo) {
			Class<?> clazz = outTypeInfo.getTypeClass();
			if (clazz == boolean[].class) {
				type = Boolean.class;
			} else if (clazz == short[].class) {
				type = Short.class;
			} else if (clazz == int[].class) {
				type = Integer.class;
			} else if (clazz == long[].class) {
				type = Long.class;
			} else if (clazz == float[].class) {
				type = Float.class;
			} else if (clazz == double[].class) {
				type = Double.class;
			} else if (clazz == char[].class) {
				type = Character.class;
			} else {
				throw new IndexOutOfBoundsException("Type could not be determined for array");
			}

		} else if (pos == 0) {
			type = outTypeInfo.getTypeClass();
		} else {
			throw new IndexOutOfBoundsException("Position is out of range");
		}
		return type;
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return discretizedStream.getExecutionConfig();
	}

	/**
	 * Gets the output type.
	 * 
	 * @return The output type.
	 */
	public TypeInformation<OUT> getType() {
		return ((StreamWindowTypeInfo<OUT>) discretizedStream.getType()).getInnerType();
	}

	private static <IN, OUT> TypeInformation<OUT> getWindowMapReturnTypes(
			WindowMapFunction<IN, OUT> windowMapInterface, TypeInformation<IN> inType) {
		return TypeExtractor.getUnaryOperatorReturnType((Function) windowMapInterface,
				WindowMapFunction.class, true, true, inType, null, false);
	}

	protected DiscretizedStream<OUT> copy() {
		return new DiscretizedStream<OUT>(discretizedStream.copy(), groupByKey, transformation,
				isPartitioned);
	}

	@Override
	public WindowedDataStream<OUT> local() {
		throw new UnsupportedOperationException(
				"Local discretisation can only be applied after defining the discretisation logic");
	}

}
