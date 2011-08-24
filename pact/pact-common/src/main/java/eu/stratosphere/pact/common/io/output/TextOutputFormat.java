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

package eu.stratosphere.pact.common.io.output;

import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Base implementation for delimiter based output formats. It can be adjusted to specific
 * formats by implementing the writeLine() method.
 * 
 * @author Moritz Kaufmann
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public abstract class TextOutputFormat<K extends Key, V extends Value> extends FileOutputFormat<K, V>
{
	/**
	 * Serializes the given key/value pair and returns it as byte stream. The delimiter
	 * between different pairs has to be included at the end of the byte array (e.g.
	 * '\n' for line based formats).
	 * 
	 * @param pair The pair to be serialized.
	 * @return The serialized KeyValuePair.
	 */
	public abstract byte[] writeLine(KeyValuePair<K, V> pair);


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(KeyValuePair<K, V> pair)
	{
		byte[] line = writeLine(pair);
		try {
			this.stream.write(line, 0, line.length);
		}
		catch (IOException e) {
			throw new RuntimeException("I/O error while writing pair to file: " + e.getMessage(), e);
		}
	}
}
