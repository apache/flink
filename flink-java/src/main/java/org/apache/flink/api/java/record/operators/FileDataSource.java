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


package org.apache.flink.api.java.record.operators;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.operators.base.FileDataSourceBase;
import org.apache.flink.types.Record;

/**
 * 
 * <b>NOTE: The Record API is marked as deprecated. It is not being developed anymore and will be removed from
 * the code at some point.
 * See <a href="https://issues.apache.org/jira/browse/FLINK-1106">FLINK-1106</a> for more details.</b>
 * 
 * Operator for input nodes which read data from files. (For Record data model)
 */
@Deprecated
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
