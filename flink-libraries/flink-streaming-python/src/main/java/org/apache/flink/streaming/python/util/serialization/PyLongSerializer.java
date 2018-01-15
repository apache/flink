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
import org.python.core.PyLong;

import java.math.BigInteger;

/**
 * A {@link Serializer} implementation for {@link PyLong} class type.
 */
public class PyLongSerializer extends Serializer<PyLong> {
	@Override
	public void write(Kryo kryo, Output output, PyLong object) {
		byte[] data = object.getValue().toByteArray();
		output.writeShort(data.length);
		output.writeBytes(data);
	}

	@Override
	public PyLong read(Kryo kryo, Input input, Class<PyLong> type) {
		int length = input.readShort();
		return new PyLong(new BigInteger(input.readBytes(length)));
	}
}
