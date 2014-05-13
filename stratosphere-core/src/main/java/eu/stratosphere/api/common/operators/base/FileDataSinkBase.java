/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.operators.base;

import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.types.Nothing;

/**
 * Operator for nodes which act as data sinks, storing the data they receive in a file instead of sending it to another
 * contract. The encoding of the data in the file is handled by the {@link eu.stratosphere.api.common.io.FileOutputFormat}.
 *
 * @see eu.stratosphere.api.common.io.FileOutputFormat
 */
public class FileDataSinkBase<IN> extends GenericDataSinkBase<IN> {

	protected final String filePath;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a FileDataSink with the provided {@link eu.stratosphere.api.common.io.FileOutputFormat} implementation
	 * and the given name, writing to the file indicated by the given path.
	 *
	 * @param f The {@link eu.stratosphere.api.common.io.FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSinkBase(FileOutputFormat<IN> f, UnaryOperatorInformation<IN, Nothing> operatorInfo, String filePath, String name) {
		super(f, operatorInfo, name);
		this.filePath = filePath;
		this.parameters.setString(FileOutputFormat.FILE_PARAMETER_KEY, filePath);
	}

	/**
	 * Creates a FileDataSink with the provided {@link eu.stratosphere.api.common.io.FileOutputFormat} implementation
	 * and the given name, writing to the file indicated by the given path.
	 *
	 * @param f The {@link eu.stratosphere.api.common.io.FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSinkBase(UserCodeWrapper<FileOutputFormat<IN>> f, UnaryOperatorInformation<IN, Nothing> operatorInfo, String filePath, String name) {
		super(f, operatorInfo, name);
		this.filePath = filePath;
		this.parameters.setString(FileOutputFormat.FILE_PARAMETER_KEY, filePath);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the configured file path where the output is written to.
	 * 
	 * @return The path to which the output shall be written.
	 */
	public String getFilePath()
	{
		return this.filePath;
	}
	

	@Override
	public String toString() {
		return this.filePath;
	}
	
}
