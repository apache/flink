/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.streaming.plan;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.python.api.types.CustomTypeWrapper;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.flink.python.api.streaming.data.PythonReceiver.createTuple;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_BOOLEAN;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_BYTE;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_BYTES;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_DOUBLE;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_FLOAT;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_INTEGER;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_LONG;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_NULL;
import static org.apache.flink.python.api.streaming.util.SerializationUtils.TYPE_STRING;

/**
 * Instances of this class can be used to receive data from the plan process.
 */
public class PythonPlanReceiver {
	private final DataInputStream input;

	public PythonPlanReceiver(InputStream input) {
		this.input = new DataInputStream(input);
	}

	public Object getRecord() throws IOException {
		return getRecord(false);
	}

	public Object getRecord(boolean normalized) throws IOException {
		return getDeserializer().deserialize(normalized);
	}

	private Deserializer getDeserializer() throws IOException {
		byte type = input.readByte();
		if (type >= 0 && type < 26) {
				Deserializer[] d = new Deserializer[type];
				for (int x = 0; x < d.length; x++) {
					d[x] = getDeserializer();
				}
				return new TupleDeserializer(d);
		}
		switch (type) {
			case TYPE_BOOLEAN:
				return new BooleanDeserializer();
			case TYPE_BYTE:
				return new ByteDeserializer();
			case TYPE_INTEGER:
				return new IntDeserializer();
			case TYPE_LONG:
				return new LongDeserializer();
			case TYPE_FLOAT:
				return new FloatDeserializer();
			case TYPE_DOUBLE:
				return new DoubleDeserializer();
			case TYPE_STRING:
				return new StringDeserializer();
			case TYPE_BYTES:
				return new BytesDeserializer();
			case TYPE_NULL:
				return new NullDeserializer();
			default:
				return new CustomTypeDeserializer(type);
		}
	}

	private abstract static class Deserializer<T> {

		public T deserialize() throws IOException {
			return deserialize(false);
		}

		public abstract T deserialize(boolean normalized) throws IOException;
	}

	private static class TupleDeserializer extends Deserializer<Tuple> {
		private final  Deserializer[] deserializer;

		public TupleDeserializer(Deserializer[] deserializer) {
			this.deserializer = deserializer;
		}

		@Override
		public Tuple deserialize(boolean normalized) throws IOException {
			Tuple result = createTuple(deserializer.length);
			for (int x = 0; x < result.getArity(); x++) {
				result.setField(deserializer[x].deserialize(normalized), x);
			}
			return result;
		}
	}

	private class CustomTypeDeserializer extends Deserializer<CustomTypeWrapper> {
		private final byte type;

		public CustomTypeDeserializer(byte type) {
			this.type = type;
		}

		@Override
		public CustomTypeWrapper deserialize(boolean normalized) throws IOException {
			int size = input.readInt();
			byte[] data = new byte[size];
			input.readFully(data);
			return new CustomTypeWrapper(type, data);
		}
	}

	private class BooleanDeserializer extends Deserializer<Boolean> {
		@Override
		public Boolean deserialize(boolean normalized) throws IOException {
			return input.readBoolean();
		}
	}

	private class ByteDeserializer extends Deserializer<Byte> {
		@Override
		public Byte deserialize(boolean normalized) throws IOException {
			return input.readByte();
		}
	}

	private class IntDeserializer extends Deserializer<Integer> {
		@Override
		public Integer deserialize(boolean normalized) throws IOException {
			return input.readInt();
		}
	}

	private class LongDeserializer extends Deserializer<Object> {
		@Override
		public Object deserialize(boolean normalized) throws IOException {
			if (normalized) {
				return new Long(input.readLong()).intValue();
			} else {
				return input.readLong();
			}
		}
	}

	private class FloatDeserializer extends Deserializer<Object> {
		@Override
		public Object deserialize(boolean normalized) throws IOException {
			if (normalized) {
				return (double) input.readFloat();
			} else {
				return input.readFloat();
			}
		}
	}

	private class DoubleDeserializer extends Deserializer<Double> {
		@Override
		public Double deserialize(boolean normalized) throws IOException {
			return input.readDouble();
		}
	}

	private class StringDeserializer extends Deserializer<String> {
		@Override
		public String deserialize(boolean normalized) throws IOException {
			int size = input.readInt();
			byte[] buffer = new byte[size];
			input.readFully(buffer);
			return new String(buffer, ConfigConstants.DEFAULT_CHARSET);
		}
	}

	private class NullDeserializer extends Deserializer<Object> {
		@Override
		public Object deserialize(boolean normalized) throws IOException {
			return null;
		}
	}

	private class BytesDeserializer extends Deserializer<byte[]> {
		@Override
		public byte[] deserialize(boolean normalized) throws IOException {
			int size = input.readInt();
			byte[] buffer = new byte[size];
			input.readFully(buffer);
			return buffer;
		}
	}
}
