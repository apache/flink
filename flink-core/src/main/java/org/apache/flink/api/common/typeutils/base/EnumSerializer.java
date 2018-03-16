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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public final class EnumSerializer<T extends Enum<T>> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	private final Class<T> enumClass;

	/**
	 * Maintain our own map of enum value to their ordinal, instead of directly using {@link Enum#ordinal}.
	 * This allows us to maintain backwards compatibility for previous serialized data in the case that the
	 * order of enum constants was changed or new constants were added.
	 *
	 * <p>On a fresh start with no reconfiguration, the ordinals would simply be identical to the enum
	 * constants actual ordinals. Ordinals may change after reconfiguration.
	 */
	private Map<T, Integer> valueToOrdinal;

	/**
	 * Array of enum constants with their indexes identical to their ordinals in the {@link #valueToOrdinal} map.
	 * Serves as a bidirectional map to have fast access from ordinal to value. May be reordered after reconfiguration.
	 */
	private T[] values;

	public EnumSerializer(Class<T> enumClass) {
		this.enumClass = checkNotNull(enumClass);
		checkArgument(Enum.class.isAssignableFrom(enumClass), "not an enum");

		this.values = enumClass.getEnumConstants();
		checkArgument(this.values.length > 0, "cannot use an empty enum");

		this.valueToOrdinal = new HashMap<>(values.length);
		int i = 0;
		for (T value : values) {
			this.valueToOrdinal.put(value, i++);
		}
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public EnumSerializer<T> duplicate() {
		return this;
	}

	@Override
	public T createInstance() {
		return values[0];
	}

	@Override
	public T copy(T from) {
		return from;
	}

	@Override
	public T copy(T from, T reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 4;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		// use our own maintained ordinals instead of the actual enum ordinal
		target.writeInt(valueToOrdinal.get(record));
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		return values[source.readInt()];
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return values[source.readInt()];
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 4);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof EnumSerializer) {
			EnumSerializer<?> other = (EnumSerializer<?>) obj;

			return other.canEqual(this) && other.enumClass == this.enumClass;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof EnumSerializer;
	}

	@Override
	public int hashCode() {
		return enumClass.hashCode();
	}

	// --------------------------------------------------------------------------------------------

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		// may be null if this serializer was deserialized from an older version
		if (this.values == null) {
			this.values = enumClass.getEnumConstants();

			this.valueToOrdinal = new HashMap<>(values.length);
			int i = 0;
			for (T value : values) {
				this.valueToOrdinal.put(value, i++);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public EnumSerializerConfigSnapshot<T> snapshotConfiguration() {
		return new EnumSerializerConfigSnapshot<>(enumClass, values);
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof EnumSerializerConfigSnapshot) {
			final EnumSerializerConfigSnapshot<T> config = (EnumSerializerConfigSnapshot<T>) configSnapshot;

			if (enumClass.equals(config.getTypeClass())) {

				T[] reorderedEnumConstants = (T[]) Array.newInstance(enumClass, this.values.length);
				Map<T, Integer> rebuiltEnumConstantToOrdinalMap = new HashMap<>(this.values.length);

				List<String> previousEnumConstants = config.getEnumConstants();

				if (previousEnumConstants.size() <= this.values.length) {
					for (int i = 0; i < previousEnumConstants.size(); i++) {
						String previousEnumConstantStr = previousEnumConstants.get(i);

						try {
							// fetch the actual enum, and use it to populate the reconstructed bi-directional map
							T enumConstant = Enum.valueOf(enumClass, previousEnumConstantStr);

							reorderedEnumConstants[i] = enumConstant;
							rebuiltEnumConstantToOrdinalMap.put(enumConstant, i);
						} catch (IllegalArgumentException e) {
							// a previous enum constant no longer exists, and therefore requires migration
							return CompatibilityResult.requiresMigration();
						}
					}
				} else {
					// some enum constants have been removed (because there are
					// fewer constants now), and therefore requires migration
					return CompatibilityResult.requiresMigration();
				}

				// if there are new enum constants, append them to the end
				if (this.values.length > previousEnumConstants.size()) {
					int appendedNewOrdinal = previousEnumConstants.size();
					for (T currentEnumConstant : this.values) {
						if (!rebuiltEnumConstantToOrdinalMap.containsKey(currentEnumConstant)) {
							reorderedEnumConstants[appendedNewOrdinal] = currentEnumConstant;
							rebuiltEnumConstantToOrdinalMap.put(currentEnumConstant, appendedNewOrdinal);
							appendedNewOrdinal++;
						}
					}
				}

				// if we reach here, we can simply reconfigure ourselves to be compatible
				this.values = reorderedEnumConstants;
				this.valueToOrdinal = rebuiltEnumConstantToOrdinalMap;
				return CompatibilityResult.compatible();
			}
		}

		return CompatibilityResult.requiresMigration();
	}

	/**
	 * Configuration snapshot of a serializer for enumerations.
	 *
	 * Configuration contains the enum class, and an array of the enum's constants
	 * that existed when the configuration snapshot was taken.
	 *
	 * @param <T> the enum type.
	 */
	public static final class EnumSerializerConfigSnapshot<T extends Enum<T>>
			extends GenericTypeSerializerConfigSnapshot<T> {

		private static final int VERSION = 2;

		private List<String> enumConstants;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public EnumSerializerConfigSnapshot() {}

		public EnumSerializerConfigSnapshot(Class<T> enumClass, T[] enumConstantsArr) {
			super(enumClass);
			this.enumConstants = buildEnumConstantsList(Preconditions.checkNotNull(enumConstantsArr));
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			out.writeInt(enumConstants.size());
			for (String enumConstant : enumConstants) {
				out.writeUTF(enumConstant);
			}
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			if (getReadVersion() == 1) {
				try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
					try {
						T[] legacyEnumConstants = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
						this.enumConstants = buildEnumConstantsList(legacyEnumConstants);
					} catch (ClassNotFoundException e) {
						throw new IOException("The requested enum class cannot be found in classpath.", e);
					} catch (IllegalArgumentException e) {
						throw new IOException("A previously existing enum constant of "
							+ getTypeClass().getName() + " no longer exists.", e);
					}
				}
			} else if (getReadVersion() == VERSION) {
				int numEnumConstants = in.readInt();

				this.enumConstants = new ArrayList<>(numEnumConstants);
				for (int i = 0; i < numEnumConstants; i++) {
					enumConstants.add(in.readUTF());
				}
			} else {
				throw new IOException("Cannot deserialize EnumSerializerConfigSnapshot with version " + getReadVersion());
			}
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public int[] getCompatibleVersions() {
			return new int[] {VERSION, 1};
		}

		public List<String> getEnumConstants() {
			return enumConstants;
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj) && enumConstants.equals(((EnumSerializerConfigSnapshot) obj).getEnumConstants());
		}

		@Override
		public int hashCode() {
			return super.hashCode() * 31 + enumConstants.hashCode();
		}

		private static <T extends Enum<T>> List<String> buildEnumConstantsList(T[] enumConstantsArr) {
			List<String> res = new ArrayList<>(enumConstantsArr.length);

			for (T enumConstant : enumConstantsArr) {
				res.add(enumConstant.name());
			}

			return res;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Test utilities
	// --------------------------------------------------------------------------------------------

	@VisibleForTesting
	T[] getValues() {
		return values;
	}

	@VisibleForTesting
	Map<T, Integer> getValueToOrdinal() {
		return valueToOrdinal;
	}
}
