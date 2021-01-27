package org.apache.flink.runtime.state.proxy;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/***/
public class ProxyKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
	// wrapped keyed state backend, either HeapKeyedStateBackend or RocksDBKeyedStateBackend
	AbstractKeyedStateBackend<K> keyedStateBackend;

	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) ProxyValueState::create),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) ProxyListState::create),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) ProxyReducingState::create),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) ProxyAggregatingState::create),
			Tuple2.of(MapStateDescriptor.class, (StateFactory) ProxyMapState::create)
		)
			.collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	public ProxyKeyedStateBackend(AbstractKeyedStateBackend<K> keyedStateBackend) {
		super(
			keyedStateBackend.kvStateRegistry,
			keyedStateBackend.keySerializer,
			keyedStateBackend.userCodeClassLoader,
			keyedStateBackend.executionConfig,
			keyedStateBackend.ttlTimeProvider,
			keyedStateBackend.cancelStreamRegistry,
			keyedStateBackend.keyGroupCompressionDecorator,
			keyedStateBackend.keyContext);
		this.keyedStateBackend = keyedStateBackend;
	}

	@Override
	public int numKeyValueStateEntries() {
		return keyedStateBackend.numKeyValueStateEntries();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		keyedStateBackend.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		return keyedStateBackend.getKeys(state, namespace);
	}

	@Override
	public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
		return keyedStateBackend.getKeysAndNamespaces(state);
	}

	@Nonnull
	@Override
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
			@Nonnull TypeSerializer<N> namespaceSerializer,
			@Nonnull StateDescriptor<S, SV> stateDesc,
			@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message =
				String.format(
					"State %s is not supported by %s",
					stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}

		return stateFactory
			.create(
				keyedStateBackend
					.createInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory));
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(
			@Nonnull String stateName,
			@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		return keyedStateBackend.create(stateName, byteOrderedElementSerializer);
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
			long checkpointId,
			long timestamp,
			@Nonnull CheckpointStreamFactory streamFactory,
			@Nonnull CheckpointOptions checkpointOptions) throws Exception {
		return keyedStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	// Factory function interface
	private interface StateFactory {
		<K, N, SV, S extends State, IS extends S> IS create(InternalKvState<K, N, SV> kvState)
			throws Exception;
	}

	@Override
	public void dispose() {
		keyedStateBackend.dispose();

		IOUtils.closeQuietly(cancelStreamRegistry);

		if (kvStateRegistry != null) {
			kvStateRegistry.unregisterAll();
		}

		lastName = null;
		lastState = null;
		keyValueStatesByName.clear();
	}

	@Override
	public <N, S extends State, T> void applyToAllKeys(
			final N namespace,
			final TypeSerializer<N> namespaceSerializer,
			final StateDescriptor<S, T> stateDescriptor,
			final KeyedStateFunction<K, S> function)
			throws Exception {

		if (keyedStateBackend instanceof HeapKeyedStateBackend) {
			try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

				// we copy the keys into list to avoid the concurrency problem
				// when state.clear() is invoked in function.process().
				final List<K> keys = keyStream.collect(Collectors.toList());

				final S state = getPartitionedState(namespace, namespaceSerializer, stateDescriptor);

				for (K key : keys) {
					setCurrentKey(key);
					function.process(key, state);
				}
			}
		} else {
			super.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
		}
	}


	/*************************************************
	 * implements CheckpointableKeyedStateBackend, Deprecated, extending AbstractKeyedStateBackend<K> instead
	 */
//	@Override
//	public KeyGroupRange getKeyGroupRange() {
//		return keyedStateBackend.getKeyGroupRange();
//	}
//
//	@Override
//	public void close() throws IOException {
//		keyedStateBackend.close();
//	}
//
//	@Override
//	public void setCurrentKey(K newKey) {
//		keyedStateBackend.setCurrentKey(newKey);
//	}
//
//	@Override
//	public K getCurrentKey() {
//		return keyedStateBackend.getCurrentKey();
//	}
//
//	@Override
//	public TypeSerializer<K> getKeySerializer() {
//		return keyedStateBackend.getKeySerializer();
//	}
//
//	@Override
//	public <N, S extends State, T> void applyToAllKeys(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor, KeyedStateFunction<K, S> function) throws Exception {
//		// this seems to change to states as well, but with very limited usage. would this change forwarded to state change log as well?
//		keyedStateBackend.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
//	}
//
//	@Override
//	public <N> Stream<K> getKeys(String state, N namespace) {
//		return keyedStateBackend.getKeys(state, namespace);
//	}
//
//	@Override
//	public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
//		return keyedStateBackend.getKeysAndNamespaces(state);
//	}
//
//	@Override
//	public <N, S extends State, T> S getOrCreateKeyedState(TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor) throws Exception {
//		return keyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
//	}
//
//	@Override
//	public <N, S extends State> S getPartitionedState(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) throws Exception {
//		// TODO, this needs to be proxied as well?
//		return keyedStateBackend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
//	}
//
//	@Override
//	public void dispose() {
//		keyedStateBackend.dispose();
//	}
//
//	@Override
//	public void registerKeySelectionListener(KeySelectionListener<K> listener) {
//		keyedStateBackend.registerKeySelectionListener(listener);
//	}
//
//	@Override
//	public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
//		return keyedStateBackend.deregisterKeySelectionListener(listener);
//	}
//
//	@Nonnull
//	@Override
//	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(@Nonnull TypeSerializer<N> namespaceSerializer, @Nonnull StateDescriptor<S, SV> stateDesc, @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
//		return null;
//	}
//
//	@Nonnull
//	@Override
//	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(@Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
//		// TODO: need more investigate whether this needs proxy?
//		return keyedStateBackend.create(stateName, byteOrderedElementSerializer);
//	}
//
//	@Nonnull
//	@Override
//	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(long checkpointId, long timestamp, @Nonnull CheckpointStreamFactory streamFactory, @Nonnull CheckpointOptions checkpointOptions) throws Exception {
//		return keyedStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
//	}
}
