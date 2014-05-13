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

package eu.stratosphere.api.java.record.operators;

import eu.stratosphere.api.common.io.FileInputFormat;
import eu.stratosphere.api.common.operators.base.FileDataSourceBase;
import eu.stratosphere.types.Record;

/**
 * Operator for input nodes which read data from files. (For Record data model)
 */
public class FileDataSource extends FileDataSourceBase<Record> {

	/**
	 * Creates a new instance for the given file using the given file input format.
	 * 
	 * @param f The {@link FileInputFormat} implementation used to read the data.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public FileDataSource(FileInputFormat<Record> f, String filePath, String name) {
		super(f, OperatorInfoHelper.source(), filePath, name);
	}

	/**
	 * Creates a new instance for the given file using the given input format. The contract has the default name.
	 * 
	 * @param f The {@link FileInputFormat} implementation used to read the data.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 */
	public FileDataSource(FileInputFormat<Record> f, String filePath) {
		super(f, OperatorInfoHelper.source(), filePath);
	}
	
	/**
	 * Creates a new instance for the given file using the given file input format.
	 * 
	 * @param f The {@link FileInputFormat} implementation used to read the data.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public FileDataSource(Class<? extends FileInputFormat<Record>> f, String filePath, String name) {
		super(f, OperatorInfoHelper.source(), filePath, name);
	}

	/**
	 * Creates a new instance for the given file using the given input format. The contract has the default name.
	 * 
	 * @param f The {@link FileInputFormat} implementation used to read the data.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 */
	public FileDataSource(Class<? extends FileInputFormat<Record>> f, String filePath) {
		super(f, OperatorInfoHelper.source(), filePath);
	}
}
