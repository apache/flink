package org.apache.flink.runtime.state.proxy;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.Collection;
import java.util.List;

public class ProxyListState<K, N, V> implements InternalListState<K, N, V> {
	private final InternalListState<K, N, V> listState;

	ProxyListState(InternalListState<K, N, V> listState) {
		this.listState = listState;
	}

	// State Merging
	@Override
	public void update(List<V> values) throws Exception {
		// Here is where change is forwarded to StateChangeLog,
		listState.update(values);
	}

	@Override
	public void addAll(List<V> values) throws Exception {
		// Here is where change is forwarded to StateChangeLog
		listState.update(values);
	}

	@Override
	public void updateInternal(List<V> valueToStore) throws Exception {
		// Here is where change is forwarded to StateChangeLog
		listState.updateInternal(valueToStore);
	}

	@Override
	public void add(V value) throws Exception {
		// Here is where change is forwarded to StateChangeLog
		listState.add(value);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		listState.mergeNamespaces(target, sources);
	}

	@Override
	public List<V> getInternal() throws Exception {
		return listState.getInternal();
	}

	@Override
	public Iterable<V> get() throws Exception {
		return listState.get();
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return listState.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return listState.getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<List<V>> getValueSerializer() {
		return listState.getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		listState.setCurrentNamespace(namespace);
	}

	@Override
	public byte[] getSerializedValue(
			byte[] serializedKeyAndNamespace,
			TypeSerializer<K> safeKeySerializer,
			TypeSerializer<N> safeNamespaceSerializer,
			TypeSerializer<List<V>> safeValueSerializer) throws Exception {
		return listState.getSerializedValue(
			serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
	}

	@Override
	public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return listState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
	}

	@Override
	public void clear() {
		listState.clear();
	}

	@SuppressWarnings("unchecked")
	static <E, K, N, SV, S extends State, IS extends S> IS create(InternalKvState<K, N, SV> listState) {
		return (IS) new ProxyListState<>((InternalListState<K, N, SV>) listState);
	}
}
