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

import net.razorvine.pickle.Unpickler;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
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
