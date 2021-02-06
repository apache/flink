package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;

/**
 * Responsible for serialization of currentKey, currentGroup and namespace.
 * Will reuse the previous serialized currentKeyed if possible.
 *
 * @param <K> type of the key.
 */
@NotThreadSafe
@Internal
public class RemoteHeapSerializedCompositeKeyBuilder<K> {

	/** The serializer for the key. */
	@Nonnull
	private final TypeSerializer<K> keySerializer;

	/** The output to write the key into. */
	@Nonnull
	private final DataOutputSerializer keyOutView;

	/** The number of Key-group-prefix bytes for the key. */
	@Nonnegative
	private final int keyGroupPrefixBytes;

	/** This flag indicates whether the key type has a variable byte size in serialization. */
	private final boolean keySerializerTypeVariableSized;

	/** Mark for the position after the serialized key. */
	@Nonnegative
	private int afterKeyMark;

	public RemoteHeapSerializedCompositeKeyBuilder(
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnegative int initialSize) {
		this(
			keySerializer,
			new DataOutputSerializer(initialSize),
			keyGroupPrefixBytes,
			RemoteHeapKeySerializationUtils.isSerializerTypeVariableSized(keySerializer),
			0);
	}

	@VisibleForTesting
	RemoteHeapSerializedCompositeKeyBuilder(
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull DataOutputSerializer keyOutView,
		@Nonnegative int keyGroupPrefixBytes,
		boolean keySerializerTypeVariableSized,
		@Nonnegative int afterKeyMark) {
		this.keySerializer = keySerializer;
		this.keyOutView = keyOutView;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.keySerializerTypeVariableSized = keySerializerTypeVariableSized;
		this.afterKeyMark = afterKeyMark;
	}

	/**
	 * Sets the key and key-group as prefix. This will serialize them into the buffer and the will be used to create
	 * composite keys with provided namespaces.
	 *
	 * @param key the key.
	 * @param keyGroupId the key-group id for the key.
	 */
	public void setKeyAndKeyGroup(@Nonnull K key, @Nonnegative int keyGroupId) {
		try {
			serializeKeyGroupAndKey(key, keyGroupId);
		} catch (IOException shouldNeverHappen) {
			throw new FlinkRuntimeException(shouldNeverHappen);
		}
	}

	/**
	 * Returns a serialized composite key, from the key and key-group provided in a previous call to
	 * {@link #setKeyAndKeyGroup(Object, int)} and the given namespace.
	 *
	 * @param namespace the namespace to concatenate for the serialized composite key bytes.
	 * @param namespaceSerializer the serializer to obtain the serialized form of the namespace.
	 * @param <N> the type of the namespace.
	 *
	 * @return the bytes for the serialized composite key of key-group, key, namespace.
	 */
	@Nonnull
	public <N> byte[] buildCompositeKeyNamespace(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer) {
		try {
			serializeNamespace(namespace, namespaceSerializer);
			final byte[] result = keyOutView.getCopyOfBuffer();
			resetToKey();
			return result;
		} catch (IOException shouldNeverHappen) {
			throw new FlinkRuntimeException(shouldNeverHappen);
		}
	}

	/**
	 * Returns a serialized composite key, from the key and key-group provided in a previous call to
	 * {@link #setKeyAndKeyGroup(Object, int)} and the given namespace, folloed by the given user-key.
	 *
	 * @param namespace the namespace to concatenate for the serialized composite key bytes.
	 * @param namespaceSerializer the serializer to obtain the serialized form of the namespace.
	 * @param userKey the user-key to concatenate for the serialized composite key, after the namespace.
	 * @param userKeySerializer the serializer to obtain the serialized form of the user-key.
	 * @param <N> the type of the namespace.
	 * @param <UK> the type of the user-key.
	 *
	 * @return the bytes for the serialized composite key of key-group, key, namespace.
	 */
	@Nonnull
	public <N, UK> byte[] buildCompositeKeyNamesSpaceUserKey(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull UK userKey,
		@Nonnull TypeSerializer<UK> userKeySerializer) throws IOException {
		serializeNamespace(namespace, namespaceSerializer);
		userKeySerializer.serialize(userKey, keyOutView);
		byte[] result = keyOutView.getCopyOfBuffer();
		resetToKey();
		return result;
	}

	@Nonnull
	public <N, UK> byte[] buildCompositeKeyNamesSpaceDesc(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		byte[] desc) throws IOException {
		serializeNamespace(namespace, namespaceSerializer);
		keyOutView.write(desc);
		byte[] result = keyOutView.getCopyOfBuffer();
		resetToKey();
		return result;
	}


	@Nonnull
	public <N, UK> byte[] buildCompositeKeyUserKey(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull UK userKey,
		@Nonnull TypeSerializer<UK> userKeySerializer) throws IOException {
		userKeySerializer.serialize(userKey, keyOutView);
		byte[] result = keyOutView.getCopyOfBuffer();
		resetToKey();
		return result;
	}

	private void serializeKeyGroupAndKey(K key, int keyGroupId) throws IOException {

		// clear buffer and mark
		resetFully();

		// write key-group
		RemoteHeapKeySerializationUtils.writeKeyGroup(
			keyGroupId,
			keyGroupPrefixBytes,
			keyOutView);
		// write key
		keySerializer.serialize(key, keyOutView);
		afterKeyMark = keyOutView.length();
	}

	private <N> void serializeNamespace(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer) throws IOException {

		// this should only be called when there is already a key written so that we build the composite.
		assert isKeyWritten();

		final boolean ambiguousCompositeKeyPossible = isAmbiguousCompositeKeyPossible(
			namespaceSerializer);
		if (ambiguousCompositeKeyPossible) {
			RemoteHeapKeySerializationUtils.writeVariableIntBytes(
				afterKeyMark - keyGroupPrefixBytes,
				keyOutView);
		}
		RemoteHeapKeySerializationUtils.writeNameSpace(
			namespace,
			namespaceSerializer,
			keyOutView,
			ambiguousCompositeKeyPossible);
	}

	private void resetFully() {
		afterKeyMark = 0;
		keyOutView.clear();
	}

	private void resetToKey() {
		keyOutView.setPosition(afterKeyMark);
	}

	private boolean isKeyWritten() {
		return afterKeyMark > 0;
	}

	@VisibleForTesting
	boolean isAmbiguousCompositeKeyPossible(TypeSerializer<?> namespaceSerializer) {
		return keySerializerTypeVariableSized &
			RemoteHeapKeySerializationUtils.isSerializerTypeVariableSized(namespaceSerializer);
	}
}
