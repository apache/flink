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

package org.apache.flink.streaming.python.util.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.python.core.PyObject;

import java.io.IOException;

/**
 * A {@link Serializer} implementation for {@link PyObject} class type.
 */
public class PyObjectSerializer extends Serializer<PyObject> {

	public void write(Kryo kryo, Output output, PyObject po) {
		try {
			byte[] serPo = SerializationUtils.serializeObject(po);
			output.writeInt(serPo.length);
			output.write(serPo);
		} catch (IOException e) {
			throw new KryoException("Failed to serialize object.", e);
		}
	}

	public PyObject read(Kryo kryo, Input input, Class<PyObject> type) {
		int len = input.readInt();
		byte[] serPo = input.readBytes(len);
		try {
			return (PyObject) SerializationUtils.deserializeObject(serPo);
		} catch (IOException e) {
			throw new KryoException("Failed to deserialize object.", e);
		} catch (ClassNotFoundException e) {
			// this should only be possible if jython isn't on the class-path
			throw new KryoException("Failed to deserialize object.", e);
		}
	}
}

