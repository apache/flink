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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * DataSinkTask which is executed by a Nephele task manager.
 * The task writes data to files and uses an OutputFormat to serialize KeyValuePairs to a binary data stream.
 * Currently, the distributed Hadoop Filesystem (HDFS) is the only supported data storage.
 * 
 * @see eu.stratosphere.pact.common.io.OutputFormat
 * @author Fabian Hueske
 */
@SuppressWarnings("unchecked")
public class DataSinkTask extends AbstractFileOutputTask {

	// Obtain DataSinkTask Logger
	private static final Log LOG = LogFactory.getLog(DataSinkTask.class);

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// OutputFormat instance
	private OutputFormat format;

	// task configuration
	private Config config;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// initialize OutputFormat
		initOutputFormat();
		// initialize input reader
		initInputReader();

		LOG.debug("Finished registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {
		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		FSDataOutputStream fdos = null;

		Path path = getFileOutputPath();
		FileSystem fs = path.getFileSystem();

		if (fs.exists(path) && fs.getFileStatus(path).isDir()) {
			// write output in directory
			path = path.suffix("/" + (this.getEnvironment().getIndexInSubtaskGroup() + 1));
		}

		fdos = fs.create(path, true);

		LOG.debug("Start writing output to " + path.toString() + " : " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		format.setOutput(fdos);
		format.open();

		while (reader.hasNext()) {
			KeyValuePair pair = reader.next();
			format.writePair(pair);
			// byte[] line = format.writeLine(pair);
			// fdos.write(line, 0, line.length);
		}

		format.close();
		fdos.close(); // Should this be done in the format?l

		LOG.debug("Finished writing output to " + path.toString() + " : " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		if (fdos != null) {
			fdos.close();
		}

		LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * Initializes the OutputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of OutputFormat implementation can not be
	 *         obtained.
	 */
	private void initOutputFormat() throws RuntimeException {

		// obtain task configuration (including stub parameters)
		config = new Config(getRuntimeConfiguration());

		// obtain stub implementation class
		ClassLoader cl;
		try {
			cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends OutputFormat> formatClass = config.getStubClass(OutputFormat.class, cl);
			// obtain instance of stub implementation
			format = formatClass.newInstance();
			// configure stub implementation
			format.configure(config.getStubParameters());

		} catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("OutputFormat implementation class was not found.", cnfe);
		} catch (InstantiationException ie) {
			throw new RuntimeException("OutputFormat implementation could not be instanciated.", ie);
		} catch (IllegalAccessException iae) {
			throw new RuntimeException("OutputFormat implementations nullary constructor is not accessible.", iae);
		}
	}

	/**
	 * Initializes the input reader of the DataSinkTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader() throws RuntimeException {

		// create RecordDeserializer
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer(
			format.getOutKeyType(), format.getOutValueType());

		// determine distribution pattern for reader from input ship strategy
		DistributionPattern dp = null;
		switch (config.getInputShipStrategy(0)) {
		case FORWARD:
			// forward requires Pointwise DP
			dp = new PointwiseDistributionPattern();
			break;
		default:
			throw new RuntimeException("No valid input ship strategy provided for MapTask.");
		}

		// create reader
		// map has only one input, so we create one reader (id=0).
		reader = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer, dp);

	}

	public static class Config extends TaskConfig {

		private static final String FORMAT_CLASS = "formatClass";

		private static final String FILE_PATH = "outputPath";

		public Config(Configuration config) {
			super(config);
		}

		@Override
		public void setStubClass(Class<? extends Stub<?, ?>> formatClass) {
			config.setString(FORMAT_CLASS, formatClass.getName());
		}

		@Override
		public <T extends Stub<?, ?>> Class<? extends T> getStubClass(Class<T> formatClass, ClassLoader cl)
				throws ClassNotFoundException {
			String formatClassName = config.getString(FORMAT_CLASS, null);
			if (formatClassName == null) {
				throw new IllegalStateException("format class missing");
			}

			return Class.forName(formatClassName, true, cl).asSubclass(formatClass);
		}

		public void setFilePath(String filePath) {
			config.setString(FILE_PATH, filePath);
		}

		public String getFilePath() {
			return config.getString(FILE_PATH, null);
		}
	}

}
