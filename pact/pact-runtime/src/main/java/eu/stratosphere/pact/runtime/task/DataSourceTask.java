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

package eu.stratosphere.pact.runtime.task;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.hdfs.DistributedDataInputStream;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * DataSourceTask which is executed by a Nephele task manager.
 * The task reads data and uses an InputFormat to create KeyValuePairs from the read data.
 * Currently, the distributed Hadoop Filesystem (HDFS) is the only supported data storage.
 * 
 * @see eu.stratosphere.pact.common.io.InputFormat
 * @author Moritz Kaufmann
 * @author Fabian Hueske
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class DataSourceTask extends AbstractFileInputTask {

	// Obtain DataSourceTask Logger
	private static final Log LOG = LogFactory.getLog(DataSourceTask.class);

	// Output collector
	private OutputCollector<Key, Value> output;

	// InputFormat instance
	private InputFormat<Key, Value> format;

	// Task configuration
	private Config config;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// Initialize InputFormat
		initInputFormat();

		// Initialize OutputCollector
		initOutputCollector();

		LOG.debug("Finished registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {
		KeyValuePair<Key, Value> pair = null;

		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// get file splits to read
		FileInputSplit[] splits = getFileInputSplits();

		// set object creation policy to immutable
		boolean immutable = config.getMutability() == Config.Mutability.IMMUTABLE;

		// for each assigned input split
		for (int i = 0; i < splits.length; i++) {

			// get start and end
			FileInputSplit split = splits[i];
			long start = split.getStart();
			long length = split.getLength();

			LOG.debug("Opening input split " + split.getPath() + " : " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			// create line reader
			FileSystem fs = FileSystem.get(split.getPath().toUri(), new org.apache.hadoop.conf.Configuration());
			FSDataInputStream fdis = fs.open(new Path(split.getPath().toUri().toString()));

			// set input stream of input format
			format.setInput(new DistributedDataInputStream(fdis), start, length, (1024 * 1024));

			// open input format
			format.open();

			LOG.debug("Starting reader on file " + split.getPath() + " : " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			// create mutable pair once
			if (!immutable) {
				pair = format.createPair();
			}

			// as long as there is data to read
			while (!format.reachedEnd()) {

				// create immutable pair
				if (immutable) {
					pair = format.createPair();
				}

				// build next pair
				boolean valid = format.nextPair(pair);

				// ship pair if it is valid
				if (valid) {
					output.collect(pair.getKey(), pair.getValue());
				}
			}

			// close the input stream
			format.close();

			LOG.debug("Closing input split " + split.getPath() + " : " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		}

		LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

	}

	/**
	 * Initializes the InputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of InputFormat implementation can not be
	 *         obtained.
	 */
	private void initInputFormat() {

		// obtain task configuration (including stub parameters)
		config = new Config(getRuntimeConfiguration());

		// obtain stub implementation class
		try {
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends InputFormat> formatClass = config.getStubClass(InputFormat.class, cl);
			// obtain instance of stub implementation
			format = formatClass.newInstance();
			// configure stub implementation
			format.configure(config.getStubParameters());

		} catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("InputFormat implementation class was not found.", cnfe);
		} catch (InstantiationException ie) {
			throw new RuntimeException("InputFormat implementation could not be instanciated.", ie);
		} catch (IllegalAccessException iae) {
			throw new RuntimeException("InputFormat implementations nullary constructor is not accessible.", iae);
		}
	}

	/**
	 * Creates a writer for each output. Creates an OutputCollector which
	 * forwards its input to all writers.
	 */
	private void initOutputCollector() {

		boolean fwdCopyFlag = false;
		
		// create output collector
		output = new OutputCollector<Key, Value>();
		
		// create a writer for each output
		for (int i = 0; i < config.getNumOutputs(); i++) {
			// obtain OutputEmitter from output ship strategy
			OutputEmitter oe = new OutputEmitter(config.getOutputShipStrategy(i));
			// create writer
			RecordWriter<KeyValuePair<Key, Value>> writer;
			writer = new RecordWriter<KeyValuePair<Key, Value>>(this,
				(Class<KeyValuePair<Key, Value>>) (Class<?>) KeyValuePair.class, oe);
			
			// add writer to output collector
			// the first writer does not need to send a copy
			// all following must send copies
			// TODO smarter decision are possible here, e.g. decide which channel may not need to copy, ...
			output.addWriter(writer, fwdCopyFlag);
			fwdCopyFlag = true;

		}
	}

	/**
	 * Specialized configuration object that holds parameters specific to the
	 * data-source configuration.
	 */
	public static final class Config extends TaskConfig {
		public enum Mutability {
			MUTABLE, IMMUTABLE
		};

		private static final String KEY_FILE_PATH = "inputPath";

		private static final String KEY_MUTABILITY = "pairMutability";

		private static final String KEY_FORMAT_PREFIX = "pact.datasource.format.";

		public Config(Configuration config) {
			super(config);
		}

		public void setFilePath(String filePath) {
			config.setString(KEY_FILE_PATH, filePath);
		}

		public String getFilePath() {
			return config.getString(KEY_FILE_PATH, null);
		}

		public void setMutability(Mutability mutability) {
			config.setString(KEY_MUTABILITY, mutability.name());
		}

		public Mutability getMutability() {
			return Mutability.valueOf(config.getString(KEY_MUTABILITY, Mutability.IMMUTABLE.name()));
		}

		public void setFormatParameter(String paramName, String value) {
			config.setString(KEY_FORMAT_PREFIX + paramName, value);
		}

		public String getFormatParameter(String paramName, String defaultValue) {
			return config.getString(KEY_FORMAT_PREFIX + paramName, defaultValue);
		}

		public void setFormatParameters(Configuration parameters) {
			for (String key : parameters.keySet()) {
				config.setString(KEY_FORMAT_PREFIX + key, parameters.getString(key, null));
			}
		}

		public Configuration getFormatParameters() {
			Configuration parameters = new Configuration();

			for (String key : config.keySet()) {
				if (key.startsWith(KEY_FORMAT_PREFIX)) {
					parameters.setString(key.substring(KEY_FORMAT_PREFIX.length()), config.getString(key, null));
				}
			}
			return parameters;
		}
	}
}
