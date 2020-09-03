/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalAggregateProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Type;

/**
 * A builder for creating {@link WindowOperator WindowOperators}.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <T> The type of the incoming elements.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class WindowOperatorBuilder<T, K, W extends Window> {

	private static final String WINDOW_STATE_NAME = "window-contents";

	private final ExecutionConfig config;

	private final WindowAssigner<? super T, W> windowAssigner;

	private final TypeInformation<T> inputType;

	private final KeySelector<T, K> keySelector;

	private final TypeInformation<K> keyType;

	private Trigger<? super T, ? super W> trigger;

	@Nullable
	private Evictor<? super T, ? super W> evictor;

	private long allowedLateness = 0L;

	@Nullable
	private OutputTag<T> lateDataOutputTag;

	public WindowOperatorBuilder(
		WindowAssigner<? super T, W> windowAssigner,
		Trigger<? super T, ? super W> trigger,
		ExecutionConfig config,
		TypeInformation<T> inputType,
		KeySelector<T, K> keySelector,
		TypeInformation<K> keyType) {
		this.windowAssigner = windowAssigner;
		this.config = config;
		this.inputType = inputType;
		this.keySelector = keySelector;
		this.keyType = keyType;
		this.trigger = trigger;
	}

	public void trigger(Trigger<? super T, ? super W> trigger) {
		Preconditions.checkNotNull(trigger, "Window triggers cannot be null");

		if (windowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
			throw new UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with a custom trigger.");
		}

		this.trigger = trigger;
	}

	public void allowedLateness(Time lateness) {
		Preconditions.checkNotNull(lateness, "Allowed lateness cannot be null");

		final long millis = lateness.toMilliseconds();
		Preconditions.checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

		this.allowedLateness = millis;
	}

	public void sideOutputLateData(OutputTag<T> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
		this.lateDataOutputTag = outputTag;
	}

	public void evictor(Evictor<? super T, ? super W> evictor) {
		Preconditions.checkNotNull(evictor, "Evictor cannot be null");

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with an Evictor.");
		}
		this.evictor = evictor;
	}

	public <R> WindowOperator<K, T, ?, R, W> reduce(ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function) {
		Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
		Preconditions.checkNotNull(function, "WindowFunction cannot be null");

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)));
		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>(WINDOW_STATE_NAME, reduceFunction, inputType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueWindowFunction<>(function));
		}
	}

	public <R> WindowOperator<K, T, ?, R, W> reduce(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function) {
		Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
		Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableProcessWindowFunction<>(new ReduceApplyProcessWindowFunction<>(reduceFunction, function)));
		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>(WINDOW_STATE_NAME, reduceFunction, inputType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueProcessWindowFunction<>(function));
		}
	}

	@Deprecated
	public <ACC, R> WindowOperator<K, T, ?, R, W> fold(
		ACC initialValue,
		FoldFunction<T, ACC> foldFunction,
		WindowFunction<ACC, R, K, W> function,
		TypeInformation<ACC> foldAccumulatorType) {

		Preconditions.checkNotNull(foldAccumulatorType, "FoldFunction cannot be null");
		Preconditions.checkNotNull(function, "WindowFunction cannot be null");

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a " +
				windowAssigner.getClass().getSimpleName() + " assigner.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableWindowFunction<>(new FoldApplyWindowFunction<>(initialValue, foldFunction, function, foldAccumulatorType)));
		} else {
			FoldingStateDescriptor<T, ACC> stateDesc = new FoldingStateDescriptor<>(WINDOW_STATE_NAME,
				initialValue, foldFunction, foldAccumulatorType.createSerializer(config));
			return buildWindowOperator(stateDesc, new InternalSingleValueWindowFunction<>(function));
		}
	}

	@Deprecated
	public <ACC, R> WindowOperator<K, T, ?, R, W> fold(
		ACC initialValue,
		FoldFunction<T, ACC> foldFunction,
		ProcessWindowFunction<ACC, R, K, W> windowFunction,
		TypeInformation<ACC> foldAccumulatorType) {

		Preconditions.checkNotNull(foldAccumulatorType, "FoldFunction cannot be null");
		Preconditions.checkNotNull(windowFunction, "ProcessWindowFunction cannot be null");

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a " +
				windowAssigner.getClass().getSimpleName() + " assigner.");
		}

		if (evictor != null) {
			InternalIterableProcessWindowFunction<T, R, K, W> internalFunction = new InternalIterableProcessWindowFunction<>(
				new FoldApplyProcessWindowFunction<>(initialValue, foldFunction, windowFunction, foldAccumulatorType));
			return buildEvictingWindowOperator(internalFunction);
		} else {
			FoldingStateDescriptor<T, ACC> stateDesc = new FoldingStateDescriptor<>(WINDOW_STATE_NAME,
				initialValue, foldFunction, foldAccumulatorType.createSerializer(config));
			return buildWindowOperator(stateDesc, new InternalSingleValueProcessWindowFunction<>(windowFunction));
		}
	}

	public <ACC, V, R> WindowOperator<K, T, ?, R, W> aggregate(
		AggregateFunction<T, ACC, V> aggregateFunction,
		WindowFunction<V, R, K, W> windowFunction,
		TypeInformation<ACC> accumulatorType) {

		Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
		Preconditions.checkNotNull(windowFunction, "WindowFunction cannot be null");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalIterableWindowFunction<>(new AggregateApplyWindowFunction<>(aggregateFunction, windowFunction)));
		} else {
			AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>(WINDOW_STATE_NAME,
				aggregateFunction, accumulatorType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueWindowFunction<>(windowFunction));
		}
	}

	public <ACC, V, R> WindowOperator<K, T, ?, R, W> aggregate(
		AggregateFunction<T, ACC, V> aggregateFunction,
		ProcessWindowFunction<V, R, K, W> windowFunction,
		TypeInformation<ACC> accumulatorType) {

		Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
		Preconditions.checkNotNull(windowFunction, "ProcessWindowFunction cannot be null");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		if (evictor != null) {
			return buildEvictingWindowOperator(new InternalAggregateProcessWindowFunction<>(aggregateFunction, windowFunction));
		} else {
			AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>(WINDOW_STATE_NAME,
				aggregateFunction, accumulatorType.createSerializer(config));

			return buildWindowOperator(stateDesc, new InternalSingleValueProcessWindowFunction<>(windowFunction));
		}
	}

	public <R> WindowOperator<K, T, ?, R, W> apply(WindowFunction<T, R, K, W> function) {
		Preconditions.checkNotNull(function, "WindowFunction cannot be null");
		return apply(new InternalIterableWindowFunction<>(function));
	}

	public <R> WindowOperator<K, T, ?, R, W> process(ProcessWindowFunction<T, R, K, W> function) {
		Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");
		return apply(new InternalIterableProcessWindowFunction<>(function));
	}

	private  <R> WindowOperator<K, T, ?, R, W> apply(InternalWindowFunction<Iterable<T>, R, K, W> function) {
		if (evictor != null) {
			return buildEvictingWindowOperator(function);
		} else {
			ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>(WINDOW_STATE_NAME, inputType.createSerializer(config));

			return buildWindowOperator(stateDesc, function);
		}
	}

	private <ACC, R> WindowOperator<K, T, ACC, R, W> buildWindowOperator(
		StateDescriptor<? extends AppendingState<T, ACC>, ?> stateDesc,
		InternalWindowFunction<ACC, R, K, W> function) {

		return new WindowOperator<>(
			windowAssigner,
			windowAssigner.getWindowSerializer(config),
			keySelector,
			keyType.createSerializer(config),
			stateDesc,
			function,
			trigger,
			allowedLateness,
			lateDataOutputTag);
	}

	private <R> WindowOperator<K, T, Iterable<T>, R, W> buildEvictingWindowOperator(InternalWindowFunction<Iterable<T>, R, K, W> function) {
		@SuppressWarnings({"unchecked", "rawtypes"})
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
			(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(inputType.createSerializer(config));

		ListStateDescriptor<StreamRecord<T>> stateDesc = new ListStateDescriptor<>(WINDOW_STATE_NAME, streamRecordSerializer);

		return new EvictingWindowOperator<>(windowAssigner,
			windowAssigner.getWindowSerializer(config),
			keySelector,
			keyType.createSerializer(config),
			stateDesc,
			function,
			trigger,
			evictor,
			allowedLateness,
			lateDataOutputTag);
	}

	private static String generateFunctionName(Function function) {
		Class<? extends Function> functionClass = function.getClass();
		if (functionClass.isAnonymousClass()) {
			// getSimpleName returns an empty String for anonymous classes
			Type[] interfaces = functionClass.getInterfaces();
			if (interfaces.length == 0) {
				// extends an existing class (like RichMapFunction)
				Class<?> functionSuperClass = functionClass.getSuperclass();
				return functionSuperClass.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			} else {
				// implements a Function interface
				Class<?> functionInterface = functionClass.getInterfaces()[0];
				return functionInterface.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			}
		} else {
			return functionClass.getSimpleName();
		}
	}

	public String generateOperatorName(Function function1, @Nullable Function function2) {
		return "Window(" +
			windowAssigner + ", " +
			trigger.getClass().getSimpleName() + ", " +
			(evictor == null ? "" : (evictor.getClass().getSimpleName() + ", ")) +
			generateFunctionName(function1) +
			(function2 == null ? "" : (", " + generateFunctionName(function2))) +
			")";
	}

	@VisibleForTesting
	public long getAllowedLateness() {
		return allowedLateness;
	}
}
