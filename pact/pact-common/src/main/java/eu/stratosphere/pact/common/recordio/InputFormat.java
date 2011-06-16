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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Describes the base interface that is used for reading from a input.
 * The input format handles the following:
 * <ul>
 *   <li>It describes how the input is split into splits that can be processed in parallel.</li>
 *   <li>Different parallel instances read records out of their assigned split.</li>
 * </ul>
 * 
 * @param <T> The type of input split.
 * 
 * @author Stephan Ewen
 */
public interface InputFormat<T extends InputSplit>
{
	public void configure(Configuration parameters);
	
	public T[] createInputSplits();
	
	// --------------------------------------------------------------------------------------------
	
	public void open(T split) throws IOException;
	
	/**
	 * Method used to check if the end of the input is reached.
	 * 
	 * @return True if the end is reached, otherwise false.
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	public abstract boolean reachedEnd() throws IOException;
	
	/**
	 * Tries to read the next pair from the input. By using the return value invalid records in the
	 * input can be skipped.
	 * 
	 * @param record Record into which the next key / value pair will be stored.
	 * @return Indicates whether the record could be successfully read. A return value of <i>true</i>
	 *         indicates that the read was successful, a return value of false indicates that the
	 *         current record was not read successfully and should be skipped.
	 *         
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	public boolean nextRecord(PactRecord record) throws IOException;
	
	
	public void close() throws IOException;
}
