/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.python;

import org.apache.flink.api.common.python.pickle.ArrayConstructor;
import org.apache.flink.api.common.python.pickle.ByteArrayConstructor;

import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class that contains helper methods to create a TableSource from
 * a file which contains Python objects.
 */
public final class PythonBridgeUtils {

	private static Object[] getObjectArrayFromUnpickledData(Object input) {
		if (input.getClass().isArray()) {
			return (Object[]) input;
		} else {
			return ((ArrayList<Object>) input).toArray(new Object[0]);
		}
	}

	public static List<Object[]> readPythonObjects(String fileName, boolean batched)
		throws IOException {
		List<byte[]> data = readPickledBytes(fileName);
		Unpickler unpickle = new Unpickler();
		initialize();
		List<Object[]> unpickledData = new ArrayList<>();
		for (byte[] pickledData: data) {
			Object obj = unpickle.loads(pickledData);
			if (batched) {
				if (obj instanceof Object[]) {
					Object[] arrayObj = (Object[]) obj;
					for (Object o : arrayObj) {
						unpickledData.add(getObjectArrayFromUnpickledData(o));
					}
				} else {
					for (Object o : (ArrayList<Object>) obj) {
						unpickledData.add(getObjectArrayFromUnpickledData(o));
					}
				}
			} else {
				unpickledData.add(getObjectArrayFromUnpickledData(obj));
			}
		}
		return unpickledData;
	}

	public static byte[] convertLiteralToPython(RexLiteral o, SqlTypeName typeName) {
		byte type;
		Object value;
		Pickler pickler = new Pickler();
		if (o.getValue3() == null) {
			type = 0;
			value = null;
		} else {
			switch (typeName) {
				case TINYINT:
					type = 0;
					value = ((BigDecimal) o.getValue3()).byteValueExact();
					break;
				case SMALLINT:
					type = 0;
					value = ((BigDecimal) o.getValue3()).shortValueExact();
					break;
				case INTEGER:
					type = 0;
					value = ((BigDecimal) o.getValue3()).intValueExact();
					break;
				case BIGINT:
					type = 0;
					value = ((BigDecimal) o.getValue3()).longValueExact();
					break;
				case FLOAT:
					type = 0;
					value = ((BigDecimal) o.getValue3()).floatValue();
					break;
				case DOUBLE:
					type = 0;
					value = ((BigDecimal) o.getValue3()).doubleValue();
					break;
				case DECIMAL:
				case BOOLEAN:
					type = 0;
					value = o.getValue3();
					break;
				case CHAR:
				case VARCHAR:
					type = 0;
					value = o.getValue3().toString();
					break;
				case DATE:
					type = 1;
					value = o.getValue3();
					break;
				case TIME:
					type = 2;
					value = o.getValue3();
					break;
				case TIMESTAMP:
					type = 3;
					value = o.getValue3();
					break;
				default:
					throw new RuntimeException("Unsupported type " + typeName);
			}
		}
		byte[] pickledData;
		try {
			pickledData = pickler.dumps(value);
		} catch (IOException e) {
			throw new RuntimeException("Pickle Java object failed", e);
		}
		byte[] typePickledData = new byte[pickledData.length + 1];
		typePickledData[0] = type;
		System.arraycopy(pickledData, 0, typePickledData, 1, pickledData.length);
		return typePickledData;
	}

	private static List<byte[]> readPickledBytes(final String fileName) throws IOException {
		List<byte[]> objs = new LinkedList<>();
		try (DataInputStream din = new DataInputStream(new FileInputStream(fileName))) {
			try {
				while (true) {
					final int length = din.readInt();
					byte[] obj = new byte[length];
					din.readFully(obj);
					objs.add(obj);
				}
			} catch (EOFException eof) {
				// expected
			}
		}
		return objs;
	}

	private static boolean initialized = false;
	private static void initialize() {
		synchronized (PythonBridgeUtils.class) {
			if (!initialized) {
				Unpickler.registerConstructor("array", "array", new ArrayConstructor());
				Unpickler.registerConstructor("__builtin__", "bytearray", new ByteArrayConstructor());
				Unpickler.registerConstructor("builtins", "bytearray", new ByteArrayConstructor());
				Unpickler.registerConstructor("__builtin__", "bytes", new ByteArrayConstructor());
				Unpickler.registerConstructor("_codecs", "encode", new ByteArrayConstructor());
				initialized = true;
			}
		}
	}
}
