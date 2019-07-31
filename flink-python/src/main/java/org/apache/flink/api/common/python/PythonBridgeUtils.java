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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.python.pickle.ArrayConstructor;
import org.apache.flink.api.common.python.pickle.ByteArrayConstructor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import net.razorvine.pickle.Unpickler;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class that contains helper methods to create a DataStream/DataSet from
 * a file which contains Python objects.
 */
public final class PythonBridgeUtils {

	/**
	 * Creates a DataStream from a file which contains serialized python objects.
	 */
	public static DataStream<Object[]> createDataStreamFromFile(
		final StreamExecutionEnvironment streamExecutionEnvironment,
		final String fileName,
		final boolean batched) throws IOException {
		return streamExecutionEnvironment
			.fromCollection(readPythonObjects(fileName))
			.flatMap(new PythonFlatMapFunction(batched))
			.returns(Types.GENERIC(Object[].class));
	}

	/**
	 * Creates a DataSet from a file which contains serialized python objects.
	 */
	public static DataSet<Object[]> createDataSetFromFile(
		final ExecutionEnvironment executionEnvironment,
		final String fileName,
		final boolean batched) throws IOException {
		return executionEnvironment
			.fromCollection(readPythonObjects(fileName))
			.flatMap(new PythonFlatMapFunction(batched))
			.returns(Types.GENERIC(Object[].class));
	}

	private static List<byte[]> readPythonObjects(final String fileName) throws IOException {
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

	private static final class PythonFlatMapFunction extends RichFlatMapFunction<byte[], Object[]> {

		private static final long serialVersionUID = 1L;

		private final boolean batched;
		private transient Unpickler unpickle;

		PythonFlatMapFunction(boolean batched) {
			this.batched = batched;
			initialize();
		}

		@Override
		public void open(Configuration parameters) {
			this.unpickle = new Unpickler();
		}

		@Override
		public void flatMap(byte[] value, Collector<Object[]> out) throws Exception {
			Object obj = unpickle.loads(value);
			if (batched) {
				if (obj instanceof Object[]) {
					for (int i = 0; i < ((Object[]) obj).length; i++) {
						collect(out, ((Object[]) obj)[i]);
					}
				} else {
					for (Object o : (ArrayList<Object>) obj) {
						collect(out, o);
					}
				}
			} else {
				collect(out, obj);
			}
		}

		private void collect(Collector<Object[]> out, Object obj) {
			if (obj.getClass().isArray()) {
				out.collect((Object[]) obj);
			} else {
				out.collect(((ArrayList<Object>) obj).toArray(new Object[0]));
			}
		}
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
