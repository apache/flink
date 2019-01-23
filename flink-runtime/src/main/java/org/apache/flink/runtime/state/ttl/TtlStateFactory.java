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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This state factory wraps state objects, produced by backends, with TTL logic.
 */
public class TtlStateFactory<K, N, SV, TTLSV, S extends State, IS extends S> {
	public static <K, N, SV, TTLSV, S extends State, IS extends S> IS createStateAndWrapWithTtlIfEnabled(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, SV> stateDesc,
		KeyedStateBackend<K> stateBackend,
		TtlTimeProvider timeProvider) throws Exception {
		Preconditions.checkNotNull(namespaceSerializer);
		Preconditions.checkNotNull(stateDesc);
		Preconditions.checkNotNull(stateBackend);
		Preconditions.checkNotNull(timeProvider);
		return  stateDesc.getTtlConfig().isEnabled() ?
			new TtlStateFactory<K, N, SV, TTLSV, S, IS>(
				namespaceSerializer, stateDesc, stateBackend, timeProvider)
				.createState() :
			stateBackend.createInternalState(namespaceSerializer, stateDesc);
	}

	private final Map<Class<? extends StateDescriptor>, SupplierWithException<IS, Exception>> stateFactories;

	@Nonnull
	private final TypeSerializer<N> namespaceSerializer;
	@Nonnull
	private final StateDescriptor<S, SV> stateDesc;
	@Nonnull
	private final KeyedStateBackend<K> stateBackend;
	@Nonnull
	private final StateTtlConfig ttlConfig;
	@Nonnull
	private final TtlTimeProvider timeProvider;
	private final long ttl;
	@Nullable
	private final TtlIncrementalCleanup<K, N, TTLSV> incrementalCleanup;

	private TtlStateFactory(
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull StateDescriptor<S, SV> stateDesc,
		@Nonnull KeyedStateBackend<K> stateBackend,
		@Nonnull TtlTimeProvider timeProvider) {
		this.namespaceSerializer = namespaceSerializer;
		this.stateDesc = stateDesc;
		this.stateBackend = stateBackend;
		this.ttlConfig = stateDesc.getTtlConfig();
		this.timeProvider = timeProvider;
		this.ttl = ttlConfig.getTtl().toMilliseconds();
		this.stateFactories = createStateFactories();
		this.incrementalCleanup = getTtlIncrementalCleanup();
	}

	@SuppressWarnings("deprecation")
	private Map<Class<? extends StateDescriptor>, SupplierWithException<IS, Exception>> createStateFactories() {
		return Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (SupplierWithException<IS, Exception>) this::createValueState),
			Tuple2.of(ListStateDescriptor.class, (SupplierWithException<IS, Exception>) this::createListState),
			Tuple2.of(MapStateDescriptor.class, (SupplierWithException<IS, Exception>) this::createMapState),
			Tuple2.of(ReducingStateDescriptor.class, (SupplierWithException<IS, Exception>) this::createReducingState),
			Tuple2.of(AggregatingStateDescriptor.class, (SupplierWithException<IS, Exception>) this::createAggregatingState),
			Tuple2.of(FoldingStateDescriptor.class, (SupplierWithException<IS, Exception>) this::createFoldingState)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));
	}

	@SuppressWarnings("unchecked")
	private IS createState() throws Exception {
		SupplierWithException<IS, Exception> stateFactory = stateFactories.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), TtlStateFactory.class);
			throw new FlinkRuntimeException(message);
		}
		IS state = stateFactory.get();
		if (incrementalCleanup != null) {
			incrementalCleanup.setTtlState((AbstractTtlState<K, N, ?, TTLSV, ?>) state);
		}
		return state;
	}

	@SuppressWarnings("unchecked")
	private IS createValueState() throws Exception {
		ValueStateDescriptor<TtlValue<SV>> ttlDescriptor = new ValueStateDescriptor<>(
			stateDesc.getName(), new TtlSerializer<>(stateDesc.getSerializer()));
		return (IS) new TtlValueState<>(createTtlStateContext(ttlDescriptor));
	}

	@SuppressWarnings("unchecked")
	private <T> IS createListState() throws Exception {
		ListStateDescriptor<T> listStateDesc = (ListStateDescriptor<T>) stateDesc;
		ListStateDescriptor<TtlValue<T>> ttlDescriptor = new ListStateDescriptor<>(
			stateDesc.getName(), new TtlSerializer<>(listStateDesc.getElementSerializer()));
		return (IS) new TtlListState<>(createTtlStateContext(ttlDescriptor));
	}

	@SuppressWarnings("unchecked")
	private <UK, UV> IS createMapState() throws Exception {
		MapStateDescriptor<UK, UV> mapStateDesc = (MapStateDescriptor<UK, UV>) stateDesc;
		MapStateDescriptor<UK, TtlValue<UV>> ttlDescriptor = new MapStateDescriptor<>(
			stateDesc.getName(),
			mapStateDesc.getKeySerializer(),
			new TtlSerializer<>(mapStateDesc.getValueSerializer()));
		return (IS) new TtlMapState<>(createTtlStateContext(ttlDescriptor));
	}

	@SuppressWarnings("unchecked")
	private IS createReducingState() throws Exception {
		ReducingStateDescriptor<SV> reducingStateDesc = (ReducingStateDescriptor<SV>) stateDesc;
		ReducingStateDescriptor<TtlValue<SV>> ttlDescriptor = new ReducingStateDescriptor<>(
			stateDesc.getName(),
			new TtlReduceFunction<>(reducingStateDesc.getReduceFunction(), ttlConfig, timeProvider),
			new TtlSerializer<>(stateDesc.getSerializer()));
		return (IS) new TtlReducingState<>(createTtlStateContext(ttlDescriptor));
	}

	@SuppressWarnings("unchecked")
	private <IN, OUT> IS createAggregatingState() throws Exception {
		AggregatingStateDescriptor<IN, SV, OUT> aggregatingStateDescriptor =
			(AggregatingStateDescriptor<IN, SV, OUT>) stateDesc;
		TtlAggregateFunction<IN, SV, OUT> ttlAggregateFunction = new TtlAggregateFunction<>(
			aggregatingStateDescriptor.getAggregateFunction(), ttlConfig, timeProvider);
		AggregatingStateDescriptor<IN, TtlValue<SV>, OUT> ttlDescriptor = new AggregatingStateDescriptor<>(
			stateDesc.getName(), ttlAggregateFunction, new TtlSerializer<>(stateDesc.getSerializer()));
		return (IS) new TtlAggregatingState<>(createTtlStateContext(ttlDescriptor), ttlAggregateFunction);
	}

	@SuppressWarnings({"deprecation", "unchecked"})
	private <T> IS createFoldingState() throws Exception {
		FoldingStateDescriptor<T, SV> foldingStateDescriptor = (FoldingStateDescriptor<T, SV>) stateDesc;
		SV initAcc = stateDesc.getDefaultValue();
		TtlValue<SV> ttlInitAcc = initAcc == null ? null : new TtlValue<>(initAcc, Long.MAX_VALUE);
		FoldingStateDescriptor<T, TtlValue<SV>> ttlDescriptor = new FoldingStateDescriptor<>(
			stateDesc.getName(),
			ttlInitAcc,
			new TtlFoldFunction<>(foldingStateDescriptor.getFoldFunction(), ttlConfig, timeProvider, initAcc),
			new TtlSerializer<>(stateDesc.getSerializer()));
		return (IS) new TtlFoldingState<>(createTtlStateContext(ttlDescriptor));
	}

	@SuppressWarnings("unchecked")
	private <OIS extends State, TTLS extends State, V, TTLV> TtlStateContext<OIS, V>
		createTtlStateContext(StateDescriptor<TTLS, TTLV> ttlDescriptor) throws Exception {

		OIS originalState = (OIS) stateBackend.createInternalState(
			namespaceSerializer, ttlDescriptor, getSnapshotTransformFactory());
		return new TtlStateContext<>(
			originalState, ttlConfig, timeProvider, (TypeSerializer<V>) stateDesc.getSerializer(),
			registerTtlIncrementalCleanupCallback((InternalKvState<?, ?, ?>) originalState));
	}

	private TtlIncrementalCleanup<K, N, TTLSV> getTtlIncrementalCleanup() {
		StateTtlConfig.IncrementalCleanupStrategy config =
			ttlConfig.getCleanupStrategies().getIncrementalCleanupStrategy();
		return config != null ? new TtlIncrementalCleanup<>(config.getCleanupSize()) : null;
	}

	private Runnable registerTtlIncrementalCleanupCallback(InternalKvState<?, ?, ?> originalState) {
		StateTtlConfig.IncrementalCleanupStrategy config =
			ttlConfig.getCleanupStrategies().getIncrementalCleanupStrategy();
		boolean cleanupConfigured = config != null && incrementalCleanup != null;
		boolean isCleanupActive = cleanupConfigured &&
			isStateIteratorSupported(originalState, incrementalCleanup.getCleanupSize());
		Runnable callback = isCleanupActive ? incrementalCleanup::stateAccessed : () -> { };
		if (isCleanupActive && config.runCleanupForEveryRecord()) {
			stateBackend.registerKeySelectionListener(stub -> callback.run());
		}
		return callback;
	}

	private boolean isStateIteratorSupported(InternalKvState<?, ?, ?> originalState, int size) {
		boolean stateIteratorSupported = false;
		try {
			stateIteratorSupported = originalState.getStateIncrementalVisitor(size) != null;
		} catch (Throwable t) {
			// ignore
		}
		return stateIteratorSupported;
	}

	private StateSnapshotTransformFactory<?> getSnapshotTransformFactory() {
		if (!ttlConfig.getCleanupStrategies().inFullSnapshot()) {
			return StateSnapshotTransformFactory.noTransform();
		} else {
			return new TtlStateSnapshotTransformer.Factory<>(timeProvider, ttl);
		}
	}

	/**
	 * Serializer for user state value with TTL. Visibility is public for usage with external tools.
	 */
	public static class TtlSerializer<T> extends CompositeSerializer<TtlValue<T>> {
		private static final long serialVersionUID = 131020282727167064L;

		@SuppressWarnings("WeakerAccess")
		public TtlSerializer(TypeSerializer<T> userValueSerializer) {
			super(true, LongSerializer.INSTANCE, userValueSerializer);
		}

		@SuppressWarnings("WeakerAccess")
		public TtlSerializer(PrecomputedParameters precomputed, TypeSerializer<?> ... fieldSerializers) {
			super(precomputed, fieldSerializers);
		}

		@SuppressWarnings("unchecked")
		@Override
		public TtlValue<T> createInstance(@Nonnull Object ... values) {
			Preconditions.checkArgument(values.length == 2);
			return new TtlValue<>((T) values[1], (long) values[0]);
		}

		@Override
		protected void setField(@Nonnull TtlValue<T> v, int index, Object fieldValue) {
			throw new UnsupportedOperationException("TtlValue is immutable");
		}

		@Override
		protected Object getField(@Nonnull TtlValue<T> v, int index) {
			return index == 0 ? v.getLastAccessTimestamp() : v.getUserValue();
		}

		@SuppressWarnings("unchecked")
		@Override
		protected CompositeSerializer<TtlValue<T>> createSerializerInstance(
			PrecomputedParameters precomputed,
			TypeSerializer<?> ... originalSerializers) {
			Preconditions.checkNotNull(originalSerializers);
			Preconditions.checkArgument(originalSerializers.length == 2);
			return new TtlSerializer<>(precomputed, originalSerializers);
		}
	}
}
