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

package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.table.api.dataview.MapView;

import java.util.Objects;

/**
 * TypeInformation for {@link MapView}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Internal
@Deprecated
public class MapViewTypeInfo<K, V> extends TypeInformation<MapView<K, V>> {

	private static final long serialVersionUID = -2883944144965318259L;

	private final TypeInformation<K> keyType;
	private final TypeInformation<V> valueType;
	private final boolean nullAware;
	private boolean nullSerializer;

	public MapViewTypeInfo(
		TypeInformation<K> keyType,
		TypeInformation<V> valueType,
		boolean nullSerializer,
		boolean nullAware) {

		this.keyType = keyType;
		this.valueType = valueType;
		this.nullSerializer = nullSerializer;
		this.nullAware = nullAware;
	}

	public MapViewTypeInfo(TypeInformation<K> keyType, TypeInformation<V> valueType) {
		this(keyType, valueType, false, false);
	}

	public TypeInformation<K> getKeyType() {
		return keyType;
	}

	public TypeInformation<V> getValueType() {
		return valueType;
	}

	public boolean isNullAware() {
		return nullAware;
	}

	public boolean isNullSerializer() {
		return nullSerializer;
	}

	public void setNullSerializer(boolean nullSerializer) {
		this.nullSerializer = nullSerializer;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<MapView<K, V>> getTypeClass() {
		return (Class<MapView<K, V>>) (Class<?>) MapView.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeSerializer<MapView<K, V>> createSerializer(ExecutionConfig config) {
		if (nullSerializer) {
			return (TypeSerializer<MapView<K, V>>) (TypeSerializer<?>) NullSerializer.INSTANCE;
		} else {
			TypeSerializer<K> keySer = keyType.createSerializer(config);
			TypeSerializer<V> valueSer = valueType.createSerializer(config);
			if (nullAware) {
				return new MapViewSerializer<>(new NullAwareMapSerializer<>(keySer, valueSer));
			} else {
				return new MapViewSerializer<>(new MapSerializer<>(keySer, valueSer));
			}
		}
	}

	@Override
	public String toString() {
		return "MapView<" + keyType + ", " + valueType + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof MapViewTypeInfo) {
			//noinspection unchecked
			MapViewTypeInfo<K, V> other = (MapViewTypeInfo<K, V>) obj;
			return keyType.equals(other.keyType) && valueType.equals(other.valueType)
				&& nullSerializer == other.nullSerializer && nullAware == other.nullAware;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(keyType, valueType, nullSerializer, nullAware);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj != null && obj.getClass() == getClass();
	}
}
