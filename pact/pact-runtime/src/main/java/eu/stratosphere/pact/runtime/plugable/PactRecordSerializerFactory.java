/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.plugable;

import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * A factory that create a serializer for the {@link PactRecord} data type.
 * 
 * @author Stephan Ewen
 */
public class PactRecordSerializerFactory implements TypeSerializerFactory<PactRecord>
{
	private static final PactRecordSerializerFactory INSTANCE = new PactRecordSerializerFactory();
	
	/**
	 * Gets an instance of the serializer factory. The instance is shared, since the factory is a
	 * stateless class. 
	 * 
	 * @return An instance of the serializer factory.
	 */
	public static final PactRecordSerializerFactory get() {
		return INSTANCE;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeSerializerFactory#getSerializer()
	 */
	@Override
	public TypeSerializer<PactRecord> getSerializer() {
		return PactRecordSerializer.get();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeSerializerFactory#getDataType()
	 */
	@Override
	public Class<PactRecord> getDataType() {
		return PactRecord.class;
	}
}
