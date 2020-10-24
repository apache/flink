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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A serializer used in {@link SortingDataInput} for serializing elements alongside their key and
 * timestamp. It serializes the record in a format known by the {@link FixedLengthByteKeyComparator}
 * and {@link VariableLengthByteKeyComparator}.
 *
 * <p>If the key is of known constant length, the length is not serialized with the data.
 * Therefore the serialized data is as follows:
 *
 * <pre>
 *      [key-length] | &lt;key&gt; | &lt;timestamp&gt; | &lt;record&gt;
 * </pre>
 */
final class KeyAndValueSerializer<IN> extends TypeSerializer<Tuple2<byte[], StreamRecord<IN>>> {
	private static final int TIMESTAMP_LENGTH = 8;
	private final TypeSerializer<IN> valueSerializer;

	// This represents either a variable length (-1) or a fixed one (>= 0).
	private final int serializedKeyLength;

	KeyAndValueSerializer(TypeSerializer<IN> valueSerializer, int serializedKeyLength) {
		this.valueSerializer = valueSerializer;
		this.serializedKeyLength = serializedKeyLength;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Tuple2<byte[], StreamRecord<IN>>> duplicate() {
		return new KeyAndValueSerializer<>(valueSerializer.duplicate(), this.serializedKeyLength);
	}

	@Override
	public Tuple2<byte[], StreamRecord<IN>> copy(Tuple2<byte[], StreamRecord<IN>> from) {
		StreamRecord<IN> fromRecord = from.f1;
		return Tuple2.of(
			Arrays.copyOf(from.f0, from.f0.length),
			fromRecord.copy(valueSerializer.copy(fromRecord.getValue()))
		);
	}

	@Override
	public Tuple2<byte[], StreamRecord<IN>> createInstance() {
		return Tuple2.of(new byte[0], new StreamRecord<>(valueSerializer.createInstance()));
	}

	@Override
	public Tuple2<byte[], StreamRecord<IN>> copy(
			Tuple2<byte[], StreamRecord<IN>> from,
			Tuple2<byte[], StreamRecord<IN>> reuse) {
		StreamRecord<IN> fromRecord = from.f1;
		StreamRecord<IN> reuseRecord = reuse.f1;

		IN valueCopy = valueSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
		fromRecord.copyTo(valueCopy, reuseRecord);
		reuse.f0 = Arrays.copyOf(from.f0, from.f0.length);
		reuse.f1 = reuseRecord;
		return reuse;
	}

	@Override
	public int getLength() {
		if (valueSerializer.getLength() < 0 || serializedKeyLength < 0) {
			return -1;
		}
		return valueSerializer.getLength() + serializedKeyLength + TIMESTAMP_LENGTH;
	}

	@Override
	public void serialize(Tuple2<byte[], StreamRecord<IN>> record, DataOutputView target) throws IOException {
		if (serializedKeyLength < 0) {
			target.writeInt(record.f0.length);
		}
		target.write(record.f0);
		StreamRecord<IN> toSerialize = record.f1;
		target.writeLong(toSerialize.getTimestamp());
		valueSerializer.serialize(toSerialize.getValue(), target);
	}

	@Override
	public Tuple2<byte[], StreamRecord<IN>> deserialize(DataInputView source) throws IOException {
		final int length = getKeyLength(source);
		byte[] bytes = new byte[length];
		source.read(bytes);
		long timestamp = source.readLong();
		IN value = valueSerializer.deserialize(source);
		return Tuple2.of(
			bytes,
			new StreamRecord<>(value, timestamp)
		);
	}

	@Override
	public Tuple2<byte[], StreamRecord<IN>> deserialize(Tuple2<byte[], StreamRecord<IN>> reuse, DataInputView source) throws IOException {
		final int length = getKeyLength(source);
		byte[] bytes = new byte[length];
		source.read(bytes);
		long timestamp = source.readLong();
		IN value = valueSerializer.deserialize(source);
		StreamRecord<IN> reuseRecord = reuse.f1;
		reuseRecord.replace(value, timestamp);
		reuse.f0 = bytes;
		reuse.f1 = reuseRecord;
		return reuse;
	}

	private int getKeyLength(DataInputView source) throws IOException {
		final int length;
		if (serializedKeyLength < 0) {
			length = source.readInt();
		} else {
			length = serializedKeyLength;
		}
		return length;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		final int length;
		if (serializedKeyLength < 0) {
			length = source.readInt();
			target.writeInt(length);
		} else {
			length = serializedKeyLength;
		}
		for (int i = 0; i < length; i++) {
			target.writeByte(source.readByte());
		}
		target.writeLong(source.readLong());
		valueSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		KeyAndValueSerializer<?> that = (KeyAndValueSerializer<?>) o;
		return Objects.equals(valueSerializer, that.valueSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(valueSerializer);
	}

	@Override
	public TypeSerializerSnapshot<Tuple2<byte[], StreamRecord<IN>>> snapshotConfiguration() {
		throw new UnsupportedOperationException(
			"The KeyAndValueSerializer should not be used for persisting into State!");
	}
}
