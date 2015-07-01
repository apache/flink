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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.BitSet;

public final class TupleSerializer<T extends Tuple> extends TupleSerializerBase<T> {

	private static final long serialVersionUID = 1L;

	public TupleSerializer(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
		super(tupleClass, fieldSerializers);
	}

	@Override
	public TupleSerializer<T> duplicate() {
		boolean stateful = false;
		int fieldCount = fieldSerializers.length;
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer<?>[fieldCount];

		for (int i = 0; i < fieldCount; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
			if (duplicateFieldSerializers[i] != fieldSerializers[i]) {
				// at least one of them is stateful
				stateful = true;
			}
		}

		if (stateful) {
			return new TupleSerializer<T>(tupleClass, duplicateFieldSerializers);
		} else {
			return this;
		}
	}

	@Override
	public T createInstance() {
		try {
			T t = tupleClass.newInstance();

			for (int i = 0; i < arity; i++) {
				t.setField(fieldSerializers[i].createInstance(), i);
			}

			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	public T createInstance(Object[] fields) {

		try {
			T t = tupleClass.newInstance();

			for (int i = 0; i < arity; i++) {
				t.setField(fields[i], i);
			}

			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int bitSetSize = (arity / 8) + 1;
        byte[] buffer = new byte[bitSetSize];
        source.read(buffer);
        BitSet bitIndicator = BitSet.valueOf(buffer);

        target.write(bitIndicator.toByteArray());

        for (int i = 0; i < arity; i++) {
            if (bitIndicator.get(i)) {
                fieldSerializers[i].copy(source, target);
            }
        }
    }

    @Override
	public T copy(T from) {
		T target = instantiateRaw();
		for (int i = 0; i < arity; i++) {
            Object field = from.getField(i);
            if (field != null) {
                Object copy = fieldSerializers[i].copy(field);
                target.setField(copy, i);
            } else {
                target.setField(null, i);
            }
        }
		return target;
	}

	@Override
	public T copy(T from, T reuse) {
		for (int i = 0; i < arity; i++) {
            Object field = from.getField(i);
            if (field != null) {
                Object copy = fieldSerializers[i].copy(field);
                reuse.setField(copy, i);
            } else {
                reuse.setField(null, i);
            }
		}
		return reuse;
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
        BitSet bitIndicator = new BitSet(arity);
        for (int i = 0; i < arity; i++) {
            Object o = value.getField(i);
            bitIndicator.set(i,o != null);
        }
        target.write(bitIndicator.toByteArray());

        for (int i = 0; i < arity; i++) {
            Object o = value.getField(i);
            if (o != null) {
                fieldSerializers[i].serialize(o, target);
            }
        }
    }

	@Override
	public T deserialize(DataInputView source) throws IOException {
		T tuple = instantiateRaw();

        int bitSetSize = (arity/8)+1;
        byte[] buffer = new byte[bitSetSize];
        source.read(buffer);
        BitSet bitIndicator = BitSet.valueOf(buffer);

		for (int i = 0; i < arity; i++) {
            if(!bitIndicator.get(i)){
                tuple.setField(null,i);
            } else {
                Object field = fieldSerializers[i].deserialize(source);
                tuple.setField(field, i);
            }
		}
		return tuple;
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
        int bitSetSize = (arity/8)+1;
        byte[] buffer = new byte[bitSetSize];
        source.read(buffer);
        BitSet bitIndicator = BitSet.valueOf(buffer);

		for (int i = 0; i < arity; i++) {
            if(!bitIndicator.get(i)){
                reuse.setField(null,i);
            } else {
                Object field = fieldSerializers[i].deserialize(reuse.getField(i), source);
                reuse.setField(field, i);
            }
		}
		return reuse;
	}

	private T instantiateRaw() {
		try {
			return tupleClass.newInstance();
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}
}
