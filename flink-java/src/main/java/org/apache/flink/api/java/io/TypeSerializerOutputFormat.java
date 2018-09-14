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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Stores elements by serializing them with their type serializer.
 * @param <T> type parameter
 */
@PublicEvolving
public class TypeSerializerOutputFormat<T> extends BinaryOutputFormat<T> implements InputTypeConfigurable {

	private static final long serialVersionUID = -6653022644629315158L;

	private TypeSerializer<T> serializer;

	@Override
	protected void serialize(T record, DataOutputView dataOutput) throws IOException {
		if (serializer == null){
			throw new RuntimeException("TypeSerializerOutputFormat requires a type serializer to " +
					"be defined.");
		}

		serializer.serialize(record, dataOutput);
	}

	public void setSerializer(TypeSerializer<T> serializer){
		this.serializer = serializer;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		serializer = (TypeSerializer<T>) type.createSerializer(executionConfig);
	}
}
