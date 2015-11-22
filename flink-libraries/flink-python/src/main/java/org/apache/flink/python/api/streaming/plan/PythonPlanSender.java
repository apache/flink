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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.flink.api.java.tuple.Tuple;
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
 * Instances of this class can be used to send data to the plan process.
 */
public class PythonPlanSender implements Serializable {
	private final DataOutputStream output;

	public PythonPlanSender(OutputStream output) {
		this.output = new DataOutputStream(output);
	}

	public void sendRecord(Object record) throws IOException {
		String className = record.getClass().getSimpleName().toUpperCase();
		if (className.startsWith("TUPLE")) {
			className = "TUPLE";
		}
		if (className.startsWith("BYTE[]")) {
			className = "BYTES";
		}
		SupportedTypes type = SupportedTypes.valueOf(className);
		switch (type) {
			case TUPLE:
				output.write(TYPE_TUPLE);
				int arity = ((Tuple) record).getArity();
				output.writeInt(arity);
				for (int x = 0; x < arity; x++) {
					sendRecord(((Tuple) record).getField(x));
				}
				return;
			case BOOLEAN:
				output.write(TYPE_BOOLEAN);
				output.write(((Boolean) record) ? (byte) 1 : (byte) 0);
				return;
			case BYTE:
				output.write(TYPE_BYTE);
				output.write((Byte) record);
				return;
			case BYTES:
				output.write(TYPE_BYTES);
				output.write((byte[]) record, 0, ((byte[]) record).length);
				return;
			case CHARACTER:
				output.write(TYPE_STRING);
				output.writeChars(((Character) record) + "");
				return;
			case SHORT:
				output.write(TYPE_SHORT);
				output.writeShort((Short) record);
				return;
			case INTEGER:
				output.write(TYPE_INTEGER);
				output.writeInt((Integer) record);
				return;
			case LONG:
				output.write(TYPE_LONG);
				output.writeLong((Long) record);
				return;
			case STRING:
				output.write(TYPE_STRING);
				output.writeBytes((String) record);
				return;
			case FLOAT:
				output.write(TYPE_FLOAT);
				output.writeFloat((Float) record);
				return;
			case DOUBLE:
				output.write(TYPE_DOUBLE);
				output.writeDouble((Double) record);
				return;
			case NULL:
				output.write(TYPE_NULL);
				return;
			case CUSTOMTYPEWRAPPER:
				output.write(((CustomTypeWrapper) record).getType());
				output.write(((CustomTypeWrapper) record).getData());
				return;
			default:
				throw new IllegalArgumentException("Unknown Type encountered: " + type);
		}
	}
	
	private enum SupportedTypes {
		TUPLE, BOOLEAN, BYTE, BYTES, CHARACTER, SHORT, INTEGER, LONG, FLOAT, DOUBLE, STRING, OTHER, NULL, CUSTOMTYPEWRAPPER
	}
}