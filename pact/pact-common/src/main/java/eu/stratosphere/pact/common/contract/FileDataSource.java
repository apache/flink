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

package eu.stratosphere.pact.common.contract;

import eu.stratosphere.pact.generic.io.FileInputFormat;


/**
 * Contract for input nodes which read data from files.
 */
public class FileDataSource extends GenericDataSource<FileInputFormat<?>>
{
	private static String DEFAULT_NAME = "<Unnamed File Data Source>";
	
	protected final String filePath;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new instance for the given file using the given file input format.
	 * 
	 * @param clazz The class describing the input format for the file.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public FileDataSource(Class<? extends FileInputFormat<?>> clazz, String filePath, String name)
	{
		super(clazz, name);
		this.filePath = filePath;
		this.parameters.setString(FileInputFormat.FILE_PARAMETER_KEY, filePath);
	}

	/**
	 * Creates a new instance for the given file using the given input format. The contract has the default name.
	 * 
	 * @param clazz The class describing the input format for the file.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 */
	public FileDataSource(Class<? extends FileInputFormat<?>> clazz, String file) {
		this(clazz, file, DEFAULT_NAME);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the file path from which the input is read.
	 * 
	 * @return The path from which the input shall be read.
	 */
	public String getFilePath()
	{
		return this.filePath;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		return this.filePath;
	}
}
