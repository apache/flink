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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

/**
 * Reads elements by deserializing them with a given type serializer.
 * @param <T>
 */
public class TypeSerializerInputFormat<T> extends BinaryInputFormat<T> {

	private static final long serialVersionUID = 2123068581665107480L;

	private TypeSerializer<T> serializer;

	public TypeSerializerInputFormat(TypeSerializer<T> serializer){
		this.serializer = serializer;
	}

	@Override
	protected T deserialize(T reuse, DataInputView dataInput) throws IOException {
		if(serializer == null){
			throw new RuntimeException("TypeSerializerInputFormat requires a type serializer to " +
					"be defined.");
		}

		return serializer.deserialize(reuse, dataInput);
	}
}
