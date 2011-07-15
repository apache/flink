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

package eu.stratosphere.pact.common.recordcontract;

import eu.stratosphere.pact.common.recordio.FileOutputFormat;

/**
 * Contract for nodes which act as data sinks, storing the data they receive in a file instead of sending it to another
 * contract. The encoding of the data in the file is handled by the {@link FileOutputFormat}.
 * 
 * @see FileOutputFormat
 */
public class FileDataSink extends GenericDataSink
{
	private static String DEFAULT_NAME = "<Unnamed File Data Sink>";

	protected final String filePath;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation
	 * and a default name, writing to the file indicated by the given path.
	 * 
	 * @param c The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 */
	public FileDataSink(Class<? extends FileOutputFormat> c, String filePath) {
		this(c, filePath, DEFAULT_NAME);
	}
	
	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation 
	 * and the given name, writing to the file indicated by the given path.
	 * 
	 * @param c The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSink(Class<? extends FileOutputFormat> c, String filePath, String name) {
		super(c, name);
		this.filePath = filePath;
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation the default name,
	 * writing to the file indicated by the given path. It uses the given contract as its input.
	 * 
	 * @param c The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contract to use as the input.
	 */
	public FileDataSink(Class<? extends FileOutputFormat> c, String filePath, Contract input) {
		this(c, filePath, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation and the given name,
	 * writing to the file indicated by the given path. It uses the given contract as its input.
	 * 
	 * @param c The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contract to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSink(Class<? extends FileOutputFormat> c, String filePath, Contract input, String name) {
		this(c, filePath, name);
		setInput(input);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the configured file path where the output is written to.
	 * 
	 * @return The path to which the output shall be written.
	 */
	public String getFilePath()
	{
		return filePath;
	}
}
