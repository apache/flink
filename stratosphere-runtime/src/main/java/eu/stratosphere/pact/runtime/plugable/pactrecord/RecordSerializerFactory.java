/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.plugable.pactrecord;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;

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
}
