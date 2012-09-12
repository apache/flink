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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.common.generic.io.OutputFormat;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.PactRecordNepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * DataSinkTask which is executed by a Nephele task manager.
 * The task hands the data to an output format.
 * 
 * @see eu.eu.stratosphere.pact.common.generic.io.OutputFormat
 * 
 * @author Fabian Hueske
 */
public class DataSinkTask<IT> extends AbstractOutputTask
{
	public static final String DEGREE_OF_PARALLELISM_KEY = "pact.sink.dop";
	
	public static final String SORT_ORDER = "sink.sort.order";
	
	// Obtain DataSinkTask Logger
	private static final Log LOG = LogFactory.getLog(DataSinkTask.class);

	// --------------------------------------------------------------------------------------------
	
	// OutputFormat instance. volatile, because the asynchronous canceller may access it
	private volatile OutputFormat<IT> format;
	
	// input reader
	private MutableObjectIterator<IT> reader;
	
	// The serializer for the input type
	private TypeSerializer<IT> inputTypeSerializer;

	// task configuration
	private TaskConfig config;
	
	// class loader for user code
	private ClassLoader userCodeClassLoader;

	// cancel flag
	private volatile boolean taskCanceled;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start registering input and output"));

		// initialize OutputFormat
		initOutputFormat();
		
		// initialize input reader
		initInputReader();

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Finished registering input and output"));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception
	{
		if (LOG.isInfoEnabled())
			LOG.info(getLogString("Start PACT code"));
		
		final UnilateralSortMerger<IT> sorter;
		// check whether we need to sort the output
		if (this.config.getLocalStrategy() == LocalStrategy.SORT)
		{
			final Class<? extends TypeComparatorFactory<IT>> comparatorFactoryClass = 
					this.config.getComparatorFactoryForInput(0, this.userCodeClassLoader);

			final TypeComparatorFactory<IT> comparatorFactory;
			if (comparatorFactoryClass == null) {
				// fall back to PactRecord
				@SuppressWarnings("unchecked")
				TypeComparatorFactory<IT> cf = (TypeComparatorFactory<IT>) PactRecordComparatorFactory.get();
				comparatorFactory = cf;
			} else {
				comparatorFactory = InstantiationUtil.instantiate(comparatorFactoryClass, TypeComparatorFactory.class);
			}

			TypeComparator<IT> comparator;
			try {
				comparator = comparatorFactory.createComparator(this.config.getConfigForInputParameters(0), this.userCodeClassLoader);
			} catch (ClassNotFoundException cnfex) {
				throw new Exception("The instantiation of the type comparator from factory '" +	
					comparatorFactory.getClass().getName() + 
				"' failed. A referenced class from the user code could not be loaded."); 
			}
			
			// set up memory and I/O parameters
			final long availableMemory = this.config.getMemorySize();
			final int maxFileHandles = this.config.getNumFilehandles();
			final float spillThreshold = this.config.getSortSpillingTreshold();
			
			sorter = new UnilateralSortMerger<IT>(getEnvironment().getMemoryManager(),
					getEnvironment().getIOManager(), this.reader, this, 
					this.inputTypeSerializer, comparator, availableMemory, maxFileHandles, spillThreshold);
			
			// replace the reader by the sorted input
			this.reader = sorter.getIterator();
		} else {
			sorter = null;
		}
		
		try {
			// read the reader and write it to the output
			final MutableObjectIterator<IT> reader = this.reader;
			final OutputFormat<IT> format = this.format;
			final IT record = this.inputTypeSerializer.createInstance();
			
			// check if task has been canceled
			if (this.taskCanceled) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Starting to produce output"));
			}

			// open
			format.open(this.getEnvironment().getIndexInSubtaskGroup() + 1);

			// work!
			// special case the pact record / file variant
			if (record.getClass() == PactRecord.class && format instanceof FileOutputFormat) {
				@SuppressWarnings("unchecked")
				final MutableObjectIterator<PactRecord> pi = (MutableObjectIterator<PactRecord>) reader;
				final PactRecord pr = (PactRecord) record;
				final FileOutputFormat pf = (FileOutputFormat) format;
				while (!this.taskCanceled && pi.next(pr))
				{
					pf.writeRecord(pr);
				}				
			} else {
				while (!this.taskCanceled && reader.next(record))
				{
					format.writeRecord(record);
				}
			}
			
			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (!this.taskCanceled) {
				this.format.close();
				this.format = null;
			}
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				if (LOG.isErrorEnabled())
					LOG.error(getLogString("Error in Pact user code: " + ex.getMessage()), ex);
				throw ex;
			}
		}
		finally {
			if (this.format != null) {
				// close format, if it has not been closed, yet.
				// This should only be the case if we had a previous error, or were canceled.
				try {
					this.format.close();
				}
				catch (Throwable t) {
					if (LOG.isWarnEnabled())
						LOG.warn(getLogString("Error closing the ouput format."), t);
				}
			}
			
			if (sorter != null) {
				sorter.close();
			}
		}

		if (!this.taskCanceled) {
			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Finished producing output"));

			if (LOG.isInfoEnabled())
				LOG.info(getLogString("Finished PACT code"));
		}
		else {
			if (LOG.isWarnEnabled())
				LOG.warn(getLogString("PACT code cancelled"));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		this.taskCanceled = true;
		OutputFormat<IT> format = this.format;
		if (format != null) {
			try {
				this.format.close();
			} catch (Throwable t) {}
		}
		
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
	}
	
	/**
	 * Sets the class-loader to be used to load the user code.
	 * 
	 * @param cl The class-loader to be used to load the user code.
	 */
	public void setUserCodeClassLoader(ClassLoader cl)
	{
		this.userCodeClassLoader = cl;
	}

	/**
	 * Initializes the OutputFormat implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of OutputFormat implementation can not be
	 *         obtained.
	 */
	private void initOutputFormat()
	{
		if (this.userCodeClassLoader == null) {
			try {
				this.userCodeClassLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			} catch (IOException ioe) {
				throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
			}
		}
		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getTaskConfiguration());

		// obtain stub implementation class
		try {
			@SuppressWarnings("unchecked")
			final Class<? extends OutputFormat<IT>> clazz = (Class<? extends OutputFormat<IT>>) (Class<?>) OutputFormat.class;
			final Class<? extends OutputFormat<IT>> formatClass = this.config.getStubClass(clazz, this.userCodeClassLoader);
			
			// obtain instance of stub implementation
			this.format = InstantiationUtil.instantiate(formatClass, OutputFormat.class);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("OutputFormat implementation class was not found.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("Format format class is not a proper subclass of " + OutputFormat.class.getName(), ccex); 
		}
		
		// configure the stub. catch exceptions here extra, to report them as originating from the user code 
		try {
			this.format.configure(this.config.getStubParameters());
		}
		catch (Throwable t) {
			throw new RuntimeException("The user defined 'configure()' method in the Output Format caused an error: " 
				+ t.getMessage(), t);
		}
	}

	/**
	 * Initializes the input reader of the DataSinkTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader()
	{
		// get data type serializer
		final Class<? extends TypeSerializerFactory<IT>> serializerFactoryClass;
		try {
			serializerFactoryClass = this.config.getSerializerFactoryForInput(0, this.userCodeClassLoader);
		} catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The serializer factory noted in the configuration could not be loaded.", cnfex);
		}
						
		
		final TypeSerializerFactory<IT> serializerFactory;
		if (serializerFactoryClass == null) {
			// fall back to PactRecord
			@SuppressWarnings("unchecked")
			TypeSerializerFactory<IT> ps = (TypeSerializerFactory<IT>) PactRecordSerializerFactory.get();
			serializerFactory = ps;
		} else {
			@SuppressWarnings("unchecked")
			Class<TypeSerializerFactory<IT>> clazz = (Class<TypeSerializerFactory<IT>>) (Class<?>) TypeSerializerFactory.class;
			serializerFactory = InstantiationUtil.instantiate(serializerFactoryClass, clazz);
		}
		
		this.inputTypeSerializer = serializerFactory.getSerializer();
		
		// create reader
		if (serializerFactory.getDataType().equals(PactRecord.class)) {
			@SuppressWarnings("unchecked")
			MutableObjectIterator<IT> it = (MutableObjectIterator<IT>) new PactRecordNepheleReaderIterator(new MutableRecordReader<PactRecord>(this)); 
			this.reader = it;
		} else {
			this.reader = new NepheleReaderIterator<IT>(new MutableRecordReader<DeserializationDelegate<IT>>(this), this.inputTypeSerializer);
		}
	}
	
	// ------------------------------------------------------------------------
	//                     Degree of parallelism & checks
	// ------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumNumberOfSubtasks()
	{
		if (!(this.format instanceof FileOutputFormat)) {
			return -1;
		}
		
		// ----------------- This code applies only to file inputs ------------------
		
		final String pathName = this.config.getStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, null);
		final Path path;
		
		if (pathName == null) {
			return 0;
		}
		
		try {
			path = new Path(pathName);
		}
		catch (Throwable t) {
			return 0;
		}

		// Check if the path is valid
		try {
			final FileSystem fs = path.getFileSystem();
			try {
				final FileStatus f = fs.getFileStatus(path);
				if (f == null) {
					return 1;
				}
				// If the path points to a directory we allow an infinity number of subtasks
				if (f.isDir()) {
					return -1;
				}
				else {
					// path points to an existing file. delete it to be able to replace the
					// file with a directory
					fs.delete(path, false);
					return -1;
				}
			}
			catch (FileNotFoundException fnfex) {
				// The exception is thrown if the requested file/directory does not exist.
				// if the degree of parallelism is > 1, we create a directory for this path
				int dop = getTaskConfiguration().getInteger(DEGREE_OF_PARALLELISM_KEY, -1);
				if (dop == 1) {
					// a none existing file and a degree of parallelism that is one
					return 1;
				}

				// a degree of parallelism greater one, or an unspecified one. in all cases, create a directory
				// the output
				fs.mkdirs(path);
				return -1;
			}
		}
		catch (IOException e) {
			LOG.error("Could not access the file system to detemine the status of the output.", e);
			// any other kind of I/O exception: we assume only a degree of one here
			return 1;
		}
	}

	// ------------------------------------------------------------------------
	//                               Utilities
	// ------------------------------------------------------------------------
	
	/**
	 * Utility function that composes a string for logging purposes. The string includes the given message and
	 * the index of the task in its task group together with the number of tasks in the task group.
	 * 
	 * @param message The main message for the log.
	 * @return The string ready for logging.
	 */
	private String getLogString(String message)
	{
		StringBuilder bld = new StringBuilder(128);
		bld.append(message);
		bld.append(':').append(' ');
		bld.append(this.getEnvironment().getTaskName());
		bld.append(' ').append('(');
		bld.append(this.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(this.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
	}
}
