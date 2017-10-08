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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.python.core.PyObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A serializer implementation for PyObject class type. Is is used by the Kryo serialization
 * framework. {@see https://github.com/EsotericSoftware/kryo#serializers}
 */
public class PyObjectSerializer extends Serializer<PyObject> {
	private static final Logger LOG = LoggerFactory.getLogger(PyObjectSerializer.class);

	public void write(Kryo kryo, Output output, PyObject po) {
		try {
			byte[] serPo = SerializationUtils.serializeObject(po);
			output.writeInt(serPo.length);
			output.write(serPo);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public PyObject read(Kryo kryo, Input input, Class<PyObject> type) {
		int len = input.readInt();
		byte[] serPo = new byte[len];
		input.read(serPo);
		PyObject po = null;
		try {
			po = (PyObject) SerializationUtils.deserializeObject(serPo);
		} catch (IOException | ClassNotFoundException e) {
			LOG.error(e.getMessage());
		}
		return po;
	}
}

