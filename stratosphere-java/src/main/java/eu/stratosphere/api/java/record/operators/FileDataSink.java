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

import java.util.List;

import org.apache.commons.lang3.Validate;

import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.FileDataSinkBase;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.java.typeutils.RecordTypeInfo;
import eu.stratosphere.types.Nothing;
import eu.stratosphere.types.NothingTypeInfo;
import eu.stratosphere.types.Record;

/**
 * Operator for nodes which act as data sinks, storing the data they receive in a file instead of sending it to another
 * contract. The encoding of the data in the file is handled by the {@link FileOutputFormat}.
 * 
 * @see FileOutputFormat
 */
public class FileDataSink extends FileDataSinkBase<Record> {

	private static String DEFAULT_NAME = "<Unnamed File Data Sink>";

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation 
	 * and the given name, writing to the file indicated by the given path.
	 * 
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSink(FileOutputFormat<Record> f, String filePath, String name) {
		super(f,  new UnaryOperatorInformation<Record, Nothing>(new RecordTypeInfo(), new NothingTypeInfo()), filePath, name);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation
	 * and a default name, writing to the file indicated by the given path.
	 * 
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 */
	public FileDataSink(FileOutputFormat<Record> f, String filePath) {
		this(f, filePath, DEFAULT_NAME);
	}
	
	
	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation the default name,
	 * writing to the file indicated by the given path. It uses the given contract as its input.
	 * 
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contract to use as the input.
	 */
	public FileDataSink(FileOutputFormat<Record> f, String filePath, Operator<Record> input) {
		this(f, filePath);
		setInput(input);

	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation the default name,
	 * writing to the file indicated by the given path. It uses the given contracts as its input.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contracts to use as the input.
	 * @deprecated This method will be removed in future versions. Use the {@link eu.stratosphere.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public FileDataSink(FileOutputFormat<Record> f, String filePath, List<Operator<Record>> input) {
		this(f, filePath, input, DEFAULT_NAME);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation and the given name,
	 * writing to the file indicated by the given path. It uses the given contract as its input.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contract to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSink(FileOutputFormat<Record> f, String filePath, Operator<Record> input, String name) {
		this(f, filePath, name);
		setInput(input);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation and the given name,
	 * writing to the file indicated by the given path. It uses the given contracts as its input.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contracts to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 * @deprecated This method will be removed in future versions. Use the {@link eu.stratosphere.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public FileDataSink(FileOutputFormat<Record> f, String filePath, List<Operator<Record>> input, String name) {
		this(f, filePath, name);
		Validate.notNull(input, "The input must not be null.");
		setInput(Operator.createUnionCascade(input));
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation
	 * and the given name, writing to the file indicated by the given path.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSink(Class<? extends FileOutputFormat<Record>> f, String filePath, String name) {
		super(new UserCodeClassWrapper<FileOutputFormat<Record>>(f),
				new UnaryOperatorInformation<Record, Nothing>(new RecordTypeInfo(), new NothingTypeInfo()),
				filePath, name);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation
	 * and a default name, writing to the file indicated by the given path.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 */
	public FileDataSink(Class<? extends FileOutputFormat<Record>> f, String filePath) {
		this(f, filePath, DEFAULT_NAME);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation the default name,
	 * writing to the file indicated by the given path. It uses the given contract as its input.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contract to use as the input.
	 */
	public FileDataSink(Class<? extends FileOutputFormat<Record>> f, String filePath, Operator<Record> input) {
		this(f, filePath, input, DEFAULT_NAME);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation the default name,
	 * writing to the file indicated by the given path. It uses the given contracts as its input.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contracts to use as the input.
	 * @deprecated This method will be removed in future versions. Use the {@link eu.stratosphere.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public FileDataSink(Class<? extends FileOutputFormat<Record>> f, String filePath, List<Operator<Record>> input) {
		this(f, filePath, input, DEFAULT_NAME);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation and the given name,
	 * writing to the file indicated by the given path. It uses the given contract as its input.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contract to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public FileDataSink(Class<? extends FileOutputFormat<Record>> f, String filePath, Operator<Record> input, String name) {
		this(f, filePath, name);
		setInput(input);
	}

	/**
	 * Creates a FileDataSink with the provided {@link FileOutputFormat} implementation and the given name,
	 * writing to the file indicated by the given path. It uses the given contracts as its input.
	 *
	 * @param f The {@link FileOutputFormat} implementation used to encode the data.
	 * @param filePath The path to the file to write the contents to.
	 * @param input The contracts to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 * @deprecated This method will be removed in future versions. Use the {@link eu.stratosphere.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public FileDataSink(Class<? extends FileOutputFormat<Record>> f, String filePath, List<Operator<Record>> input, String name) {
		this(f, filePath, name);
		Validate.notNull(input, "The inputs must not be null.");
		setInput(Operator.createUnionCascade(input));
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
