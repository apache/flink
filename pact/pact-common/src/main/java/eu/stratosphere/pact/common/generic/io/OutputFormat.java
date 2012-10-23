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

package eu.stratosphere.pact.common.generic.io;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;


/**
 * Describes the base interface that is used describe an output that consumes records. The output format
 * describes how to store the final records, for example in a file.
 * <p>
 * The life cycle of an output format is the following:
 * <ol>
 *   <li>After being instantiated (parameterless), it is configured with a {@link Configuration} object. 
 *       Basic fields are read from the configuration, such as for example a file path, if the format describes
 *       files as the sink for the records.</li>
 *   <li>Each parallel output task creates an instance, configures it and opens it.</li>
 *   <li>All records of its parallel instance are handed to the output format.</li>
 *   <li>The output format is closed</li>
 * </ol>
 * 
 * @author Stephan Ewen
 * 
 * @param <IT> The type of the consumed records. 
 */
public interface OutputFormat<IT>
{
	/**
	 * Configures this output format. Since output formats are instantiated generically and hence parameterless, 
	 * this method is the place where the output formats set their basic fields based on configuration values.
	 * <p>
	 * This method is always called first on a newly instantiated output format. 
	 *  
	 * @param parameters The configuration with all parameters.
	 */
	void configure(Configuration parameters);
	
	/**
	 * Opens a parallel instance of the output format to store the result of its parallel instance.
	 * <p>
	 * When this method is called, the output format it guaranteed to be configured.
	 * 
	 * @param taskNumber The number of the parallel instance.
	 * @throws IOException Thrown, if the output could not be opened due to an I/O problem.
	 */
	void open(int taskNumber) throws IOException;
	
	
	/**
	 * Adds a record to the output.
	 * <p>
	 * When this method is called, the output format it guaranteed to be opened.
	 * 
	 * @param record The records to add to the output.
	 * @throws IOException Thrown, if the records could not be added to to an I/O problem.
	 */
	void writeRecord(IT record) throws IOException;
	
	/**
	 * Method that marks the end of the life-cycle of parallel output instance. Should be used to close
	 * channels and streams and release resources.
	 * After this method returns without an error, the output is assumed to be correct.
	 * <p>
	 * When this method is called, the output format it guaranteed to be opened.
	 *  
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	void close() throws IOException;
}

