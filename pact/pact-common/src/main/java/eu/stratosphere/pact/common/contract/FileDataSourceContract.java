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

import eu.stratosphere.pact.common.io.input.FileInputFormat;
import eu.stratosphere.pact.common.io.input.InputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Contract for input nodes which read data from external data sources.
 * 
 * @author Erik Nijkamp
 * @author Moritz Kaufmann
 */
public class FileDataSourceContract<KT extends Key, VT extends Value> extends GenericDataSourceContract<KT, VT> 
{
	private static String DEFAULT_NAME = "<Unnamed File Data Source>";
	
	/**
	 * The path to the file that holds the input contents.
	 */
	protected final String filePath;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new instance for the given file using the given file input format.
	 * 
	 * @param clazz The class describing the input format for the file.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public FileDataSourceContract(Class<? extends FileInputFormat<KT, VT>> clazz, String filePath, String name)
	{
		super(clazz, name);
		this.filePath = filePath;
		setParameter(FileInputFormat.FILE_PARAMETER_KEY, filePath);
		setParameter(InputFormat.STATISTICS_CACHE_KEY, getInputIdentifier(clazz, filePath));
	}

	/**
	 * Creates a new instance for the given file using the given input format. The contract has the default name.
	 * 
	 * @param clazz The class describing the input format for the file.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 */
	public FileDataSourceContract(Class<? extends FileInputFormat<KT, VT>> clazz, String file) {
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
	
	/**
	 * Gets the class describing the input format.
	 * 
	 * @return The class describing the input format.
	 */
	@SuppressWarnings("unchecked")
	public Class<? extends FileInputFormat<KT, VT>> getFormatClass()
	{
		return (Class<FileInputFormat<KT, VT>>) this.clazz;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the identifier that is internally used to identify this input. The identifier incorporates both the
	 * input format as well as the file path.
	 * 
	 * @param inputFormat The input format used to read the file.
	 * @param filePath The path to the input file.
	 * @return An identifier for the input.
	 */
	public static final String getInputIdentifier(Class<? extends InputFormat<?, ?, ?>> inputFormat, String filePath)
	{
		return inputFormat.getName() + "|" + filePath;
	}
}
