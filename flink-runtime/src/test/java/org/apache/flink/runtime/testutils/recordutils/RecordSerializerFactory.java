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


package org.apache.flink.runtime.testutils.recordutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Record;

/**
 * A factory that create a serializer for the {@link Record} data type.
 */
public class RecordSerializerFactory implements TypeSerializerFactory<Record> {
	
	private static final RecordSerializerFactory INSTANCE = new RecordSerializerFactory();
	
	/**
	 * Gets an instance of the serializer factory. The instance is shared, since the factory is a
	 * stateless class. 
	 * 
	 * @return An instance of the serializer factory.
	 */
	public static final RecordSerializerFactory get() {
		return INSTANCE;
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public void writeParametersToConfig(Configuration config) {}

	@Override
	public void readParametersFromConfig(Configuration config, ClassLoader cl) {}
	

	@Override
	public TypeSerializer<Record> getSerializer() {
		return RecordSerializer.get();
	}

	@Override
	public Class<Record> getDataType() {
		return Record.class;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return 31;
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj != null && obj.getClass() == RecordSerializerFactory.class;
	}
}
