package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base class for composite serializers.
 *
 * <p>This class serializes a list of objects
 *
 * @param <T> type of custom serialized value
 */
@SuppressWarnings("unchecked")
public abstract class CompositeSerializer<T> extends TypeSerializer<T> {
	private final List<TypeSerializer> originalSerializers;

	protected CompositeSerializer(List<TypeSerializer> originalSerializers) {
		Preconditions.checkNotNull(originalSerializers);
		this.originalSerializers = originalSerializers;
	}

	protected abstract T composeValue(List values);

	protected abstract List decomposeValue(T v);

	protected abstract CompositeSerializer<T> createSerializerInstance(List<TypeSerializer> originalSerializers);

	private T composeValueInternal(List values) {
		Preconditions.checkArgument(values.size() == originalSerializers.size());
		return composeValue(values);
	}

	private List decomposeValueInternal(T v) {
		List values = decomposeValue(v);
		Preconditions.checkArgument(values.size() == originalSerializers.size());
		return values;
	}

	private CompositeSerializer<T> createSerializerInstanceInternal(List<TypeSerializer> originalSerializers) {
		Preconditions.checkArgument(originalSerializers.size() == originalSerializers.size());
		return createSerializerInstance(originalSerializers);
	}

	@Override
	public CompositeSerializer<T> duplicate() {
		return createSerializerInstanceInternal(originalSerializers.stream()
			.map(TypeSerializer::duplicate)
			.collect(Collectors.toList()));
	}

	@Override
	public boolean isImmutableType() {
		return originalSerializers.stream().allMatch(TypeSerializer::isImmutableType);
	}

	@Override
	public T createInstance() {
		return composeValueInternal(originalSerializers.stream()
			.map(TypeSerializer::createInstance)
			.collect(Collectors.toList()));
	}

	@Override
	public T copy(T from) {
		List originalValues = decomposeValueInternal(from);
		return composeValueInternal(
			IntStream.range(0, originalSerializers.size())
				.mapToObj(i -> originalSerializers.get(i).copy(originalValues.get(i)))
				.collect(Collectors.toList()));
	}

	@Override
	public T copy(T from, T reuse) {
		List originalFromValues = decomposeValueInternal(from);
		List originalReuseValues = decomposeValueInternal(reuse);
		return composeValueInternal(
			IntStream.range(0, originalSerializers.size())
				.mapToObj(i -> originalSerializers.get(i).copy(originalFromValues.get(i), originalReuseValues.get(i)))
				.collect(Collectors.toList()));
	}

	@Override
	public int getLength() {
		return originalSerializers.stream().allMatch(s -> s.getLength() >= 0) ?
			originalSerializers.stream().mapToInt(TypeSerializer::getLength).sum() : -1;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		List originalValues = decomposeValueInternal(record);
		for (int i = 0; i < originalSerializers.size(); i++) {
			originalSerializers.get(i).serialize(originalValues.get(i), target);
		}
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		List originalValues = new ArrayList();
		for (TypeSerializer typeSerializer : originalSerializers) {
			originalValues.add(typeSerializer.deserialize(source));
		}
		return composeValueInternal(originalValues);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		List originalValues = new ArrayList();
		List originalReuseValues = decomposeValueInternal(reuse);
		for (int i = 0; i < originalSerializers.size(); i++) {
			originalValues.add(originalSerializers.get(i).deserialize(originalReuseValues.get(i), source));
		}
		return composeValueInternal(originalValues);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		for (TypeSerializer typeSerializer : originalSerializers) {
			typeSerializer.copy(source, target);
		}
	}

	@Override
	public int hashCode() {
		return originalSerializers.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CompositeSerializer) {
			CompositeSerializer<?> other = (CompositeSerializer<?>) obj;
			return other.canEqual(this) && originalSerializers.equals(other.originalSerializers);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof CompositeSerializer;
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new CompositeTypeSerializerConfigSnapshot(originalSerializers.toArray(new TypeSerializer[]{ })) {
			@Override
			public int getVersion() {
				return 0;
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof CompositeTypeSerializerConfigSnapshot) {
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousSerializersAndConfigs =
				((CompositeTypeSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

			if (previousSerializersAndConfigs.size() == originalSerializers.size()) {

				List<TypeSerializer> convertSerializers = new ArrayList<>();
				boolean requiresMigration = false;
				CompatibilityResult<Object> compatResult;
				int i = 0;
				for (Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> f : previousSerializersAndConfigs) {
					compatResult = CompatibilityUtil.resolveCompatibilityResult(
						f.f0,
						UnloadableDummyTypeSerializer.class,
						f.f1,
						originalSerializers.get(i));

					if (compatResult.isRequiresMigration()) {
						requiresMigration = true;

						if (compatResult.getConvertDeserializer() != null) {
							convertSerializers.add(new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer()));
						} else {
							return CompatibilityResult.requiresMigration();
						}
					}

					i++;
				}

				if (!requiresMigration) {
					return CompatibilityResult.compatible();
				} else {
					return CompatibilityResult.requiresMigration(
						createSerializerInstanceInternal(convertSerializers));
				}
			}
		}

		return CompatibilityResult.requiresMigration();
	}
}
