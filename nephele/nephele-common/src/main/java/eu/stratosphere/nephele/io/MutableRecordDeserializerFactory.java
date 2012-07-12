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
 * As simple factory implementation that instantiates deserializers for mutable records.
 *
 * @author Stephan Ewen
 */
public class MutableRecordDeserializerFactory<T extends Record> implements RecordDeserializerFactory<T>
{
	/**
	 * Creates a new factory that instantiates deserializers for immutable records.
	 * 
	 * @param recordType The type of the record to be deserialized.
	 */
	public MutableRecordDeserializerFactory()
	{}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializerFactory#createDeserializer()
	 */
	@Override
	public RecordDeserializer<T> createDeserializer()
	{
		return new DefaultDeserializer<T>(null);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final RecordDeserializerFactory<Record> INSTANCE = 
									new MutableRecordDeserializerFactory<Record>();
	
	/**
	 * Gets the singleton instance of the {@code MutableRecordDeserializerFactory}.
	 * 
	 * @param <E> The generic type of the record to be deserialized.
	 * @return An instance of the factory.
	 */
	public static final <E extends Record> RecordDeserializerFactory<E> get()
	{
		@SuppressWarnings("unchecked")
		RecordDeserializerFactory<E> toReturn = (RecordDeserializerFactory<E>) INSTANCE;
		return toReturn;
	}
}
