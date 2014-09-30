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

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.types.Nothing;

/**
 * Operator for nodes which act as data sinks, storing the data they receive in a file instead of sending it to another
 * contract. The encoding of the data in the file is handled by the {@link org.apache.flink.api.common.io.FileOutputFormat}.
 *
 * @see org.apache.flink.api.common.io.FileOutputFormat
 */
public class FileDataSinkBase<IN> extends GenericDataSinkBase<IN> {

	protected final String filePath;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a FileDataSink with the provided {@link org.apache.flink.api.common.io.FileOutputFormat} implementation
	 * and the given name, writing to the file indicated by the given path.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSinkBase(FileOutputFormat<IN> f, UnaryOperatorInformation<IN, Nothing> operatorInfo, String filePath, String name) {
		super(f, operatorInfo, name);
		this.filePath = filePath;
		this.parameters.setString(FileOutputFormat.FILE_PARAMETER_KEY, filePath);
	}

	/**
	 * Creates a FileDataSink with the provided {@link org.apache.flink.api.common.io.FileOutputFormat} implementation
	 * and the given name, writing to the file indicated by the given path.
	 *
	 * @param f The {@link org.apache.flink.api.common.io.FileOutputFormat} implementation used to encode the data.
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
	public String getFilePath() {
		return this.filePath;
	}
	

	@Override
	public String toString() {
		return this.filePath;
	}
}
