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

package eu.stratosphere.pact.common.io;

import java.io.IOException;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Describes the base interface that is used for reading from a input.
 * For specific input types the createPair(), nextPair(), reachedEnd()
 * methods need to be implemented. Additionally it is advised to
 * override the open() and close() methods declared in stub to handle
 * input specific settings.
 * While reading the runtime checks whether the end was reached using reachedEnd()
 * and if not the next pair is read using the nextPair() method.
 * 
 * @author Moritz Kaufmann
 * @param <K>
 * @param <V>
 */
public abstract class InputFormat<K extends Key, V extends Value> extends Stub<K, V> {

	protected FSDataInputStream stream;

	protected long start;

	protected long length;

	protected int bufferSize;

	/**
	 * Creates a KeyValue pair that can be used together with the nextPair()
	 * method. The runtime will try to reuse the pair for several nextPair()
	 * calls.
	 * 
	 * @return A KeyValuePair to be use in the nextPair() method.
	 */
	public abstract KeyValuePair<K, V> createPair();

	/**
	 * Tries to read the next pair from the input. Must only be called if
	 * reachedEnd() is false. By using the return value invalid sequences
	 * in the input can be skipped.
	 * 
	 * @param pair
	 *        Object in which the next key / value pair will be stored
	 * @return Indicates whether the pair could be successfully read. False
	 *         does not indicate that the end is reached but that an invalid input
	 *         sequence was skipped.
	 * @throws IOException
	 *         if an I/O error occurred
	 */
	public abstract boolean nextPair(KeyValuePair<K, V> pair) throws IOException;

	/**
	 * Method used to check if the end of the input is reached.
	 * 
	 * @return returns true if the end is reached, otherwise false
	 * @throws IOException
	 *         if an I/O error occurred
	 */
	public abstract boolean reachedEnd() throws IOException;

	/**
	 * Connects the input stream to the input format.
	 * 
	 * @param fdis
	 * @param start
	 * @param length
	 * @param bufferSize
	 */
	public void setInput(FSDataInputStream fdis, long start, long length, int bufferSize) {
		this.stream = fdis;
		this.start = start;
		this.length = length;
		this.bufferSize = bufferSize;
	}

}
