package org.apache.flink.runtime.state.proxy;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import java.io.IOException;

/***/
public class ProxyValueState<K, N, V> implements InternalValueState<K, N, V> {
	InternalValueState<K, N, V> valueState;

	ProxyValueState(InternalValueState<K, N, V> valueState) {
		this.valueState = valueState;
	}

	@Override
	public V value() throws IOException {
		return valueState.value();
	}

	@Override
	public void update(V value) throws IOException {
		// Here is where change is forwarded to StateChangeLog
		valueState.update(value);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return valueState.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return valueState.getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return valueState.getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		valueState.setCurrentNamespace(namespace);
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<V> safeValueSerializer) throws Exception {
		return valueState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
	}

	@Override
	public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return valueState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
	}

	@Override
	public void clear() {
		valueState.clear();
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(InternalKvState<K, N, SV> valueState) {
		return (IS) new ProxyValueState<>((InternalValueState<K, N, SV>) valueState);
	}
}
