/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.io.channels;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.types.Record;

/**
 * This interface must be implemented by classes which transfer bytes streams back into {@link Record} objects.
 * 
 * @author Stephan Ewen
 * @param <T>
 *        The type of record this record deserializer works with.
 */
public interface RecordDeserializer<T extends Record> {

	/**
	 * Transforms a record back from a readable byte channel. The deserialization might not complete, because the
	 * channel
	 * has not all required data available. In that case, this method must return {@code null}. Furthermore, it may
	 * not retain a reference to the given target object in that case, but must manage to put the data aside.
	 * 
	 * @param target
	 *        The record object into which to deserialize the data. May be null for deserializers
	 *        that operate on immutable objects, in which case the deserializer has to instantiate an
	 *        object. In the case where this object is non-null, but the deserialization does not complete,
	 *        the object must not be used to cache the partial state, as it is not guaranteed that the object
	 *        will remain unchanged until the next attempt to continue the deserialization.
	 * @param in
	 *        The byte stream which contains the record's data.
	 * @return The record deserialized from <code>in</code>, or null, if the record .
	 * @throws IOException
	 *         Thrown if an I/O error occurs while deserializing the record from the stream
	 */
	T readData(final T target, final ReadableByteChannel readableByteChannel) throws IOException;

	/**
	 * Clears the internal buffers of the deserializer and resets its state.
	 */
	void clear();

	/**
	 * Checks whether the deserializer has data from a previous deserialization attempt stored in its internal buffers
	 * which
	 * is not yet finished.
	 * 
	 * @return <code>true</code>, if the deserializer's internal buffers contain data from a previous deserialization
	 *         attempt, <code>false</code> otherwise.
	 */
	boolean hasUnfinishedData();
}