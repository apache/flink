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

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.IOException;

/**
 * This interface must be implemented by classes which transfer bytes streams back into {@link Record} objects.
 * 
 * @author Erik Nijkamp
 * @param <T>
 *        the type of record this record deserializer works with.
 */
public interface RecordDeserializer<T extends IOReadableWritable> extends IOReadableWritable {

	/**
	 * Transforms a record back from a byte stream.
	 * 
	 * @param in
	 *        the byte stream which contains the record's data
	 * @return the record deserialized from <code>in</code>
	 * @throws IOException
	 *         thrown if an I/O error occurs while deserializing the record from the stream
	 */
	T deserialize(T target, DataInput in) throws IOException;

	/**
	 * Returns a new instance of the record deserialized by this record deserializer.
	 * 
	 * @return a new instance of the record deserialized by this deserializer
	 */
	T getInstance();

	/**
	 * Sets the class loader the record deserializer shall use.
	 * 
	 * @param classLoader
	 *        the class loader to be used by this record deserializer.
	 */
	void setClassLoader(ClassLoader classLoader);
}