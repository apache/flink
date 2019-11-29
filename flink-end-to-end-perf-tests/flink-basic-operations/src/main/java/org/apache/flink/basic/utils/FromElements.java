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

package org.apache.flink.basic.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Provides the default data sets used for the WordCount example program. The default data sets are used, if no
 * parameters are given to the program.
 */

public class FromElements {

	@SafeVarargs
	public static final <OUT> DataStreamSource<OUT> fromElements(StreamExecutionEnvironment env, Long maxCount, int sleepNum, OUT... data) {
		if (data.length == 0) {
			throw new IllegalArgumentException("fromElements needs at least one element as argument");
		}
		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(data[0]);
		} catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName() + "; please specify the TypeInformation manually via StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}
		Collection<OUT> dataCollection = Arrays.asList(data);
		FromElementsRichFunction.checkCollection(dataCollection, typeInfo.getTypeClass());
		try {
			SourceFunction<OUT> function =
				new FromElementsRichFunction<OUT>(typeInfo.createSerializer(env.getConfig()),
					maxCount, sleepNum, dataCollection);
			return env.addSource(function, "WordsData", typeInfo);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/**
	 * A stream source function that returns a sequence of elements.
	 *
	 * <p>
	 * Upon construction, this source function serializes the elements using Flink's type information. That way, any
	 * object transport using Java serialization will not be affected by the serializability of the elements.
	 * </p>
	 * <p>
	 * <b>NOTE:</b> This source has a parallelism of 1.
	 * </p>
	 *
	 * @param <T> The type of elements returned by this function.
	 */

	public static class FromElementsRichFunction<T> extends RichSourceFunction<T>
		implements ParallelSourceFunction<T>, CheckpointedFunction {
		private static final long serialVersionUID = 1L;
		private final TypeSerializer<T> serializer;
		private final byte[] elementsSerialized;
		private final int numElements;
		private volatile long numElementsEmitted;
		private volatile long numElementsToSkip;
		private volatile boolean isRunning = true;
		private Long num = 0L;
		private Long maxCount = 0L;
		private int sleepNum = 0;
		private transient ListState<Long> operateState;
		private transient Meter sourceTpsMetrics;

		public FromElementsRichFunction(TypeSerializer<T> serializer, Long maxCount, int sleepNum, T... elements) throws IOException {
			this(serializer, maxCount, sleepNum, Arrays.asList(elements));
		}

		public FromElementsRichFunction(TypeSerializer<T> serializer, Long maxCount, int sleepNum, Iterable<T> elements) throws IOException {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
			int count = 0;
			try {
				for (T element : elements) {
					serializer.serialize(element, wrapper);
					count++;
				}
			} catch (Exception e) {
				throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
			}
			this.serializer = serializer;
			this.elementsSerialized = baos.toByteArray();
			this.numElements = count;
			this.maxCount = maxCount;
			this.sleepNum = sleepNum;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			RuntimeContext runtimeContext = getRuntimeContext();
			MetricGroup metricGroup = runtimeContext.getMetricGroup();
			final String sourceTpsName = "tps";
			Counter sourceCounter = metricGroup.counter(sourceTpsName + "_counter", new SimpleCounter());
			sourceTpsMetrics = metricGroup.meter(sourceTpsName, new MeterView(sourceCounter, 60));
		}

		@Override
		public void close() {}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			Preconditions.checkState(this.operateState == null,
				"The " + getClass().getSimpleName() + " has already been initialized.");
			this.operateState = context.getOperatorStateStore().getOperatorState(
				new ListStateDescriptor<>("from-elements-state", LongSerializer.INSTANCE));
			if (context.isRestored()) {
				List<Long> retrievedStates = new ArrayList<>();
				for (Long entry : this.operateState.get()) {
					retrievedStates.add(entry);
				}
				this.numElementsToSkip = retrievedStates.get(0);
			}
		}

		@Override
		public void run(SourceContext<T> ctx) throws Exception {
			ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
			final DataInputView input = new DataInputViewStreamWrapper(bais);
			// if we are restored from a checkpoint and need to skip elements, skip them
			long toSkip = numElementsToSkip;
			if (toSkip > 0) {
				try {
					while (toSkip > 0) {
						serializer.deserialize(input);
						toSkip--;
					}
				} catch (Exception e) {
					throw new IOException("Failed to deserialize an element from the source. If you are using user-defined serialization (Value and Writable types), check the serialization functions.\nSerializer is " + serializer);
				}
				this.numElementsEmitted = this.numElementsToSkip;
			}
			final Object lock = ctx.getCheckpointLock();
			while (isRunning && num < maxCount) {
				num++;
				T next;
				try {
					Thread.sleep(sleepNum);
					if (bais.available() == 0) {
						bais.reset();
					}
					next = serializer.deserialize(input);
				} catch (Exception e) {
					throw new IOException("Failed to deserialize an element from the source. If you are using user-defined serialization (Value and Writable types), check the serialization functions.\nSerializer is " + serializer);
				}
				synchronized (lock) {
					ctx.collect(next);
					numElementsEmitted++;
					sourceTpsMetrics.markEvent();
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			Preconditions.checkState(this.operateState != null,
				"The " + getClass().getSimpleName() + " has not been properly initialized.");
			this.operateState.clear();
			if (this.numElementsEmitted != 0L) {
				this.operateState.add(this.numElementsEmitted % this.numElementsEmitted);
			} else {
				this.operateState.add(0L);
			}
		}

		/**
		 * Verifies that all elements in the collection are non-null, and are of the given class, or a subclass
		 * thereof.
		 *
		 * @param elements The collection to check.
		 * @param viewedAs The class to which the elements must be assignable to.
		 * @param <OUT>    The generic type of the collection to be checked.
		 */
		public static <OUT> void checkCollection(Collection<OUT> elements, Class<OUT> viewedAs) {
			for (OUT elem : elements) {
				if (elem == null) {
					throw new IllegalArgumentException("The collection contains a null element");
				}
				if (!viewedAs.isAssignableFrom(elem.getClass())) {
					throw new IllegalArgumentException(
						"The elements in the collection are not all subclasses of " + viewedAs.getCanonicalName());
				}
			}
		}
	}

}
