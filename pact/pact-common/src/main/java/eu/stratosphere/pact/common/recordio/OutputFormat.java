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

package eu.stratosphere.pact.common.recordio;

import java.io.IOException;

import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Describes the base interface that is used for writing to a output.
 * By overriding the createPair() and writePair() the values can be written
 * using a custom format.
 * 
 * @author Moritz Kaufmann
 * @param <K>
 * @param <V>
 */
public abstract class OutputFormat<K extends Key, V extends Value> extends Stub<K, V> {
	protected FSDataOutputStream stream;

	/**
	 * Writes the pair to the underlying output stream.
	 * 
	 * @param pair
	 * @throws IOException
	 *         if an I/O error occurred
	 */
	public abstract void writePair(KeyValuePair<K, V> pair) throws IOException;

	/**
	 * Sets the output stream to which the data should be written.
	 * 
	 * @param fdos
	 */
	public void setOutput(FSDataOutputStream fdos) {
		this.stream = fdos;
	}

}
