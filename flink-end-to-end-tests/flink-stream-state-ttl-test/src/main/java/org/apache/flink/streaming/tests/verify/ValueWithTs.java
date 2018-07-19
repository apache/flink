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

package org.apache.flink.streaming.tests.verify;

import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.io.Serializable;

/** User state value with timestamps before and after update. */
public class ValueWithTs<V> implements Serializable {
	private final V value;
	private final long timestampBeforeUpdate;
	private final long timestampAfterUpdate;

	public ValueWithTs(V value, long timestampBeforeUpdate, long timestampAfterUpdate) {
		this.value = value;
		this.timestampBeforeUpdate = timestampBeforeUpdate;
		this.timestampAfterUpdate = timestampAfterUpdate;
	}

	V getValue() {
		return value;
	}

	public long getTimestampBeforeUpdate() {
		return timestampBeforeUpdate;
	}

	public long getTimestampAfterUpdate() {
		return timestampAfterUpdate;
	}

	@Override
	public String toString() {
		return "ValueWithTs{" +
			"value=" + value +
			", timestampBeforeUpdate=" + timestampBeforeUpdate +
			", timestampAfterUpdate=" + timestampAfterUpdate +
			'}';
	}

	/** Serializer for Serializer. */
	public static class Serializer extends CompositeSerializer<ValueWithTs<?>> {

		public Serializer(TypeSerializer<?> userValueSerializer) {
			super(true, userValueSerializer, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
		}

		@SuppressWarnings("unchecked")
		Serializer(PrecomputedParameters precomputed, TypeSerializer<?>... fieldSerializers) {
			super(precomputed, fieldSerializers);
		}

		@Override
		public ValueWithTs<?> createInstance(@Nonnull Object ... values) {
			return new ValueWithTs<>(values[0], (Long) values[1], (Long) values[2]);
		}

		@Override
		protected void setField(@Nonnull ValueWithTs<?> value, int index, Object fieldValue) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected Object getField(@Nonnull ValueWithTs<?> value, int index) {
			switch (index) {
				case 0:
					return value.getValue();
				case 1:
					return value.getTimestampBeforeUpdate();
				case 2:
					return value.getTimestampAfterUpdate();
				default:
					throw new FlinkRuntimeException("Unexpected field index for ValueWithTs");
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		protected CompositeSerializer<ValueWithTs<?>> createSerializerInstance(
			PrecomputedParameters precomputed,
			TypeSerializer<?>... originalSerializers) {
			return new Serializer(precomputed, (TypeSerializer<Object>) originalSerializers[0]);
		}
	}
}
