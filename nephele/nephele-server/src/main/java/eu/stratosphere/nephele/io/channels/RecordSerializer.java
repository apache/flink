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

package eu.stratosphere.nephele.io.channels;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import eu.stratosphere.nephele.types.Record;

interface RecordSerializer<T extends Record> {

	/**
	 * Serializes the record and writes it to an internal buffer. The buffer grows dynamically
	 * in case more memory is required to serialization.
	 * 
	 * @param record
	 *        The record to the serialized
	 * @throws IOException
	 *         Thrown if data from a previous serialization process is still in the internal buffer and has not yet been
	 *         transfered to a byte buffer
	 */
	void serialize(T record) throws IOException;

	/**
	 * Return <code>true</code> if the internal serialization buffer still contains data.
	 * In this case the method serialize must not be called. If the internal buffer is empty
	 * the method return <code>false</code>
	 * 
	 * @return <code>true</code> if the internal serialization buffer still contains data, <code>false</code> it it is
	 *         empty
	 */
	boolean dataLeftFromPreviousSerialization();

	/**
	 * Reads the internal serialization buffer and writes the data to the given {@link WritableByteChannel} byte
	 * channel.
	 * 
	 * @param writableByteChannel
	 *        the channel to write the serialized data to
	 * @return <code>true<code> if more data can be written to the given buffer, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if an error occurs while writing to serialized data to the channel
	 */
	boolean read(WritableByteChannel writableByteChannel) throws IOException;

	/**
	 * Clears all the internal resources of the record serializer.
	 */
	void clear();

}
