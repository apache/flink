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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import org.apache.flink.api.java.tuple.Tuple;
import static org.apache.flink.python.api.streaming.data.PythonReceiver.createTuple;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_BOOLEAN;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_BYTE;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_BYTES;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_DOUBLE;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_FLOAT;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_INTEGER;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_LONG;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_NULL;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_SHORT;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_STRING;
import static org.apache.flink.python.api.streaming.data.PythonSender.TYPE_TUPLE;
import org.apache.flink.python.api.types.CustomTypeWrapper;

/**
 * Instances of this class can be used to receive data from the plan process.
 */
public class PythonPlanReceiver implements Serializable {
	private final DataInputStream input;

	public PythonPlanReceiver(InputStream input) {
		this.input = new DataInputStream(input);
	}

	public Object getRecord() throws IOException {
		return getRecord(false);
	}

	public Object getRecord(boolean normalized) throws IOException {
		return receiveField(normalized);
	}

	private Object receiveField(boolean normalized) throws IOException {
		byte type = (byte) input.readByte();
		switch (type) {
			case TYPE_TUPLE:
				int tupleSize = input.readByte();
				Tuple tuple = createTuple(tupleSize);
				for (int x = 0; x < tupleSize; x++) {
					tuple.setField(receiveField(normalized), x);
				}
				return tuple;
			case TYPE_BOOLEAN:
				return input.readByte() == 1;
			case TYPE_BYTE:
				return (byte) input.readByte();
			case TYPE_SHORT:
				if (normalized) {
					return (int) input.readShort();
				} else {
					return input.readShort();
				}
			case TYPE_INTEGER:
				return input.readInt();
			case TYPE_LONG:
				if (normalized) {
					return new Long(input.readLong()).intValue();
				} else {
					return input.readLong();
				}
			case TYPE_FLOAT:
				if (normalized) {
					return (double) input.readFloat();
				} else {
					return input.readFloat();
				}
			case TYPE_DOUBLE:
				return input.readDouble();
			case TYPE_STRING:
				int stringSize = input.readInt();
				byte[] string = new byte[stringSize];
				input.readFully(string);
				return new String(string);
			case TYPE_BYTES:
				int bytessize = input.readInt();
				byte[] bytes = new byte[bytessize];
				input.readFully(bytes);
				return bytes;
			case TYPE_NULL:
				return null;
			default:
				int size = input.readInt();
				byte[] data = new byte[size];
				input.readFully(data);
				return new CustomTypeWrapper(type, data);
		}
	}
}
