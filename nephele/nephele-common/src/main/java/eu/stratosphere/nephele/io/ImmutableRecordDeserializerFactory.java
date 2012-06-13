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

package eu.stratosphere.nephele.io;

import eu.stratosphere.nephele.io.channels.DefaultDeserializer;
import eu.stratosphere.nephele.types.Record;

/**
 * As simple factory implementation that instantiates deserializers for immutable records. For
 * each deserialization, a new record is instantiated from the given class.
 *
 * @author Stephan Ewen
 */
public class ImmutableRecordDeserializerFactory<T extends Record> implements RecordDeserializerFactory<T>
{
	private final Class<? extends T> recordType;			// the type of the record to be deserialized
	
	
	/**
	 * Creates a new factory that instantiates deserializers for immutable records.
	 * 
	 * @param recordType The type of the record to be deserialized.
	 */
	public ImmutableRecordDeserializerFactory(final Class<? extends T> recordType)
	{
		this.recordType = recordType;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializerFactory#createDeserializer()
	 */
	@Override
	public RecordDeserializer<T> createDeserializer()
	{
		return new DefaultDeserializer<T>(this.recordType);
	}
}
