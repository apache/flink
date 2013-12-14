/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.plugable.arrayrecord;

import eu.stratosphere.api.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.runtime.task.util.CorruptConfigurationException;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.Value;

/**
 * A factory that create a serializer for the {@link PactRecord} data type.
 */
public class ArrayRecordSerializerFactory implements TypeSerializerFactory<Value[]>
{
	private static final String NUM_FIELDS = "numfields";
	
	private static final String FIELD_CLASS_PREFIX = "fieldclass.";
	
	// --------------------------------------------------------------------------------------------
	
	private Class<? extends Value>[] types;
	
	// --------------------------------------------------------------------------------------------
	
	public ArrayRecordSerializerFactory() {
	}
	
	public ArrayRecordSerializerFactory(Class<? extends Value>[] types) {
		this.types = types;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#writeParametersToConfig(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void writeParametersToConfig(Configuration config) {
		for (int i = 0; i < this.types.length; i++) {
			if (this.types[i] == null || !Value.class.isAssignableFrom(this.types[i])) {
				throw new IllegalArgumentException("The key type " + i + " is null or not implenting the interface " + 
					Key.class.getName() + ".");
			}
		}
		
		// write the config
		config.setInteger(NUM_FIELDS, this.types.length);
		for (int i = 0; i < this.types.length; i++) {
			config.setString(FIELD_CLASS_PREFIX + i, this.types[i].getName());
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeComparatorFactory#readParametersFromConfig(eu.stratosphere.nephele.configuration.Configuration, java.lang.ClassLoader)
	 */
	@Override
	public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
		// figure out how many key fields there are
		final int numFields = config.getInteger(NUM_FIELDS, -1);
		if (numFields < 0) {
			throw new CorruptConfigurationException("The number of field for the serializer is invalid: " + numFields);
		}
		
		@SuppressWarnings("unchecked")
		final Class<? extends Value>[] types = new Class[numFields];
		
		// read the individual key positions and types
		for (int i = 0; i < numFields; i++) {
			// next type
			final String name = config.getString(FIELD_CLASS_PREFIX + i, null);
			if (name != null) {
				types[i] = Class.forName(name, true, cl).asSubclass(Key.class);
			} else {
				throw new CorruptConfigurationException("The key type (" + i + 
					") for the comparator is null"); 
			}
		}
		this.types = types;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeSerializerFactory#getSerializer()
	 */
	@Override
	public ArrayRecordSerializer getSerializer() {
		return new ArrayRecordSerializer(this.types);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeSerializerFactory#getDataType()
	 */
	@Override
	public Class<Value[]> getDataType() {
		return Value[].class;
	}
}
