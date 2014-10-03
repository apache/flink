/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.OperatorInformation;

import com.google.common.base.Preconditions;

/**
 * Operator for input nodes which read data from files.
 */
public class FileDataSourceBase<OUT> extends GenericDataSourceBase<OUT, FileInputFormat<OUT>> {

	protected final String filePath;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new instance for the given file using the given file input format.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.FileInputFormat} implementation used to read the data.
	 * @param operatorInfo The type information for the output type.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public FileDataSourceBase(FileInputFormat<OUT> f, OperatorInformation<OUT> operatorInfo, String filePath, String name) {
		super(f, operatorInfo, name);

		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		this.filePath = filePath;
		f.setFilePath(filePath);
	}

	/**
	 * Creates a new instance for the given file using the given input format. The contract has the default name.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.FileInputFormat} implementation used to read the data.
	 * @param operatorInfo The type information for the output type.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 */
	public FileDataSourceBase(FileInputFormat<OUT> f, OperatorInformation<OUT> operatorInfo, String filePath) {
		this(f, operatorInfo, Preconditions.checkNotNull(filePath, "The file path may not be null."), "File " + filePath);
	}

	/**
	 * Creates a new instance for the given file using the given file input format.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.FileInputFormat} implementation used to read the data.
	 * @param operatorInfo The type information for the output type.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public FileDataSourceBase(Class<? extends FileInputFormat<OUT>> f, OperatorInformation<OUT> operatorInfo, String filePath, String name) {
		super(f, operatorInfo, name);

		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		this.filePath = filePath;
		FileInputFormat.configureFileFormat(this).filePath(filePath);
	}

	/**
	 * Creates a new instance for the given file using the given input format. The contract has the default name.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.FileInputFormat} implementation used to read the data.
	 * @param operatorInfo The type information for the output type.
	 * @param filePath The file location. The file path must be a fully qualified URI, including the address schema.
	 */
	public FileDataSourceBase(Class<? extends FileInputFormat<OUT>> f, OperatorInformation<OUT> operatorInfo, String filePath) {
		this(f, operatorInfo, Preconditions.checkNotNull(filePath, "The file path may not be null."), "File " + filePath);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the file path from which the input is read.
	 * 
	 * @return The path from which the input shall be read.
	 */
	public String getFilePath() {
		return this.filePath;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public String toString() {
		return this.filePath;
	}
}
