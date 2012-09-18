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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.nephele.io.BroadcastRecordWriter;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.MutableUnionRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.pact.runtime.shipping.OutputCollector;
import eu.stratosphere.pact.runtime.shipping.OutputEmitter;
import eu.stratosphere.pact.runtime.shipping.PactRecordOutputCollector;
import eu.stratosphere.pact.runtime.shipping.PactRecordOutputEmitter;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.chaining.ExceptionInChainedStubException;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.PactRecordNepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehavior;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * The abstract base class for all Pact tasks. Encapsulated common behavior and implements the main life-cycle
 * of the user code.
 *
 * @author Stephan Ewen
 */
public class RegularPactTask<S extends Stub, OT> extends AbstractTask implements PactTaskContext<S, OT>
{
	protected static final Log LOG = LogFactory.getLog(RegularPactTask.class);

	protected volatile PactDriver<S, OT> driver;

	protected S stub;

	protected Collector<OT> output;

	protected List<AbstractRecordWriter<?>> eventualOutputs;

	protected MutableObjectIterator<?>[] inputs;
	
	protected MutableReader<?>[] inputReaders;

	protected TypeSerializer<?>[] inputSerializers;

	protected TypeComparator<?>[] inputComparators;
	
	protected TypeComparator<?>[] secondarySortComparators;

	protected TaskConfig config;

	protected ClassLoader userCodeClassLoader;

	protected ArrayList<ChainedDriver<?, ?>> chainedTasks;

	protected volatile boolean running = true;

	// --------------------------------------------------------------------------------------------
	//                                  Nephele Task Interface
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#registerInputOutput()
	 */
	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug(formatLogString("Start registering input and output."));
		}

		// get the classloader first. the classloader might have been set before by mock environments during testing
		if (this.userCodeClassLoader == null) {
			try {
				this.userCodeClassLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			}
			catch (IOException ioe) {
				throw new RuntimeException("The ClassLoader for the user code could not be instantiated from the library cache.", ioe);
			}
		}

		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getTaskConfiguration());

		// now get the driver class, which drives the actual pact
		final Class<? extends PactDriver<S, OT>> driverClass = this.config.getDriver();
		this.driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);

		try {
			initStub(this.driver.getStubType());
		} catch (Exception e) {
			throw new RuntimeException("Initializing the user code and the configuration failed" +
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
		}

		try {
			initInputs();
		} catch (Exception e) {
			throw new RuntimeException("Initializing the input streams failed" +
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
		}

		try {
			initOutputs();
		} catch (Exception e) {
			throw new RuntimeException("Initializing the output handlers failed" +
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(formatLogString("Finished registering input and output."));
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#invoke()
	 */
	@Override
	public void invoke() throws Exception
	{
		if (!this.running) {
			if (LOG.isDebugEnabled())
				LOG.info(formatLogString("Task cancelled before PACT code was started."));
			return;
		}
		
		if (LOG.isInfoEnabled())
			LOG.info(formatLogString("Start PACT code."));

		// setup the driver
		try {
			this.driver.setup(this);
		}
		catch (Throwable t) {
			throw new Exception("The pact driver setup for '" + this.getEnvironment().getTaskName() +
				"' , caused an error: " + t.getMessage(), t);
		}

		boolean stubOpen = false;

		try {
			// run the data preparation
			try {
				this.driver.prepare();
			}
			catch (Throwable t) {
				// if the preparation caused an error, clean up
				// errors during clean-up are swallowed, because we have already a root exception
				throw new Exception("The data preparation for task '" + this.getEnvironment().getTaskName() +
					"' , caused an error: " + t.getMessage(), t);
			}
			
			// check for canceling
			if (!this.running) {
				return;
			}

			// start all chained tasks
			RegularPactTask.openChainedTasks(this.chainedTasks, this);

			// open stub implementation
			try {
				Configuration stubConfig = this.config.getStubParameters();
				stubConfig.setInteger("pact.parallel.task.id", this.getEnvironment().getIndexInSubtaskGroup());
				stubConfig.setInteger("pact.parallel.task.count", this.getEnvironment().getCurrentNumberOfSubtasks());
				if (this.getEnvironment().getTaskName() != null) {
					stubConfig.setString("pact.parallel.task.name", this.getEnvironment().getTaskName());
				}
				this.stub.open(stubConfig);
				stubOpen = true;
			}
			catch (Throwable t) {
				throw new Exception("The user defined 'open()' method caused an exception: " + t.getMessage(), t);
			}

			// run the user code
			this.driver.run();

			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (this.running) {
				this.stub.close();
				stubOpen = false;
			}

			this.output.close();

			// close all chained tasks letting them report failure
			RegularPactTask.closeChainedTasks(this.chainedTasks, this);
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			if (stubOpen) {
				try {
					this.stub.close();
				}
				catch (Throwable t) {}
			}

			RegularPactTask.cancelChainedTasks(this.chainedTasks);

			// drop exception, if the task was canceled
			if (this.running) {
				RegularPactTask.logAndThrowException(ex, this);
			}
		}
		finally {
			this.driver.cleanup();
		}

		if (this.running) {
			if (LOG.isInfoEnabled())
				LOG.info(formatLogString("Finished PACT code."));
		}
		else {
			if (LOG.isWarnEnabled())
				LOG.warn(formatLogString("PACT code cancelled."));
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		this.running = false;
		if (LOG.isWarnEnabled())
			LOG.warn(formatLogString("Cancelling PACT code"));
		
		if (this.driver != null) {
			this.driver.cancel();
		}
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

	// --------------------------------------------------------------------------------------------
	//                                 Task Setup and Teardown
	// --------------------------------------------------------------------------------------------

	/**
	 * Initializes the Stub class implementation and configuration.
	 *
	 * @throws RuntimeException Thrown, if the stub class could not be loaded, instantiated,
	 *                          or caused an exception while being configured.
	 */
	protected void initStub(Class<? super S> stubSuperClass) throws Exception
	{
		// obtain stub implementation class
		try {
			@SuppressWarnings("unchecked")
			Class<S> stubClass = (Class<S>) this.config.getStubClass(stubSuperClass, this.userCodeClassLoader);
			this.stub = InstantiationUtil.instantiate(stubClass, stubSuperClass);
		}
		catch (ClassNotFoundException cnfe) {
			throw new Exception("The stub implementation class was not found.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new Exception("The stub class is not a proper subclass of " + stubSuperClass.getName(), ccex);
		}
	}

	/**
	 * Creates the record readers for the number of inputs as defined by {@link #getNumberOfInputs()}.
	 *
	 * This method requires that the task configuration, the driver, and the user-code class loader are set.
	 */
	protected void initInputs() throws Exception
	{
		final int numInputs = this.driver.getNumberOfInputs();

		final MutableObjectIterator<?>[] inputs = new MutableObjectIterator[numInputs];
		final MutableReader<?>[] inputReaders = new MutableReader[numInputs];
		
		final TypeSerializer<?>[] inputSerializers = new TypeSerializer[numInputs];
		final TypeComparator<?>[] inputComparators = this.driver.requiresComparatorOnInput() ?
											new TypeComparator[numInputs] : null;
		final TypeComparator<?>[] secondarySortComparators = this.driver.requiresComparatorOnInput() ? 
											new TypeComparator[numInputs] : null;

		for (int i = 0; i < numInputs; i++)
		{
			//  ---------------- create the serializer first ---------------------
			final Class<? extends TypeSerializerFactory<?>> serializerFactoryClass =
									this.config.getSerializerFactoryForInput(i, this.userCodeClassLoader);

			final TypeSerializerFactory<?> serializerFactory;
			if (serializerFactoryClass == null) {
				// fall back to PactRecord
				serializerFactory = PactRecordSerializerFactory.get();
			} else {
				serializerFactory = InstantiationUtil.instantiate(serializerFactoryClass, TypeSerializerFactory.class);
			}

			inputSerializers[i] = serializerFactory.getSerializer();

			//  ---------------- create the input stream ---------------------
			// in case the input unions multiple inputs, create a union reader
			final int groupSize = this.config.getGroupSize(i+1);
			if (groupSize < 2) {
				// non-union case
				if (serializerFactory.getDataType() == PactRecord.class) {
					// have a special case for the PactRecord serialization
					final MutableRecordReader<PactRecord> reader = new MutableRecordReader<PactRecord>(this);
					inputReaders[i] = reader;
					inputs[i] = new PactRecordNepheleReaderIterator(reader, readerInterruptionBehavior());
				} else {
					// generic data type serialization
					final MutableRecordReader<DeserializationDelegate<?>> reader =
													new MutableRecordReader<DeserializationDelegate<?>>(this);
					inputReaders[i] = reader;
					@SuppressWarnings({ "unchecked", "rawtypes" })
					final MutableObjectIterator<?> iter = new NepheleReaderIterator(reader, inputSerializers[i],
						readerInterruptionBehavior());
					inputs[i] = iter;
				}
			} else {
				// union case
				if (serializerFactory.getDataType() == PactRecord.class) {
					// have a special case for the PactRecord serialization
					@SuppressWarnings("unchecked")
					MutableRecordReader<PactRecord>[] readers = new MutableRecordReader[groupSize];
					for (int j = 0; j < groupSize; ++j) {
						readers[j] = new MutableRecordReader<PactRecord>(this);
					}
					final MutableUnionRecordReader<PactRecord> reader = new MutableUnionRecordReader<PactRecord>(readers);
					inputReaders[i] = reader;
					inputs[i] = new PactRecordNepheleReaderIterator(reader, readerInterruptionBehavior());
				} else {
					@SuppressWarnings("unchecked")
					MutableRecordReader<DeserializationDelegate<?>>[] readers = new MutableRecordReader[groupSize];
					for (int j = 0; j < groupSize; ++j) {
						readers[j] = new MutableRecordReader<DeserializationDelegate<?>>(this);
					}
					final MutableUnionRecordReader<DeserializationDelegate<?>> reader = new MutableUnionRecordReader<DeserializationDelegate<?>>(readers);
					inputReaders[i] = reader;
					
					@SuppressWarnings({ "unchecked", "rawtypes" })
					final MutableObjectIterator<?> iter = new NepheleReaderIterator(reader, inputSerializers[i],
						readerInterruptionBehavior());
					inputs[i] = iter;
				}
			}

			//  ---------------- create the comparator ---------------------
			if (this.driver.requiresComparatorOnInput()) {
				final Class<? extends TypeComparatorFactory<?>> comparatorFactoryClass =
							this.config.getComparatorFactoryForInput(i, this.userCodeClassLoader);

				final TypeComparatorFactory<?> comparatorFactory;
				if (comparatorFactoryClass == null) {
					// fall back to PactRecord
					comparatorFactory = PactRecordComparatorFactory.get();
				} else {
					comparatorFactory = InstantiationUtil.instantiate(comparatorFactoryClass, TypeComparatorFactory.class);
				}

				try {
					inputComparators[i] = comparatorFactory.createComparator(this.config.getConfigForInputParameters(i), this.userCodeClassLoader);
					secondarySortComparators[i] = comparatorFactory.createSecondarySortComparator(this.config.getConfigForInputParameters(i), this.userCodeClassLoader);
				} catch (ClassNotFoundException cnfex) {
					throw new Exception("The instantiation of the type comparator from factory '" +
						comparatorFactory.getClass().getName() +
						"' failed. A referenced class from the user code could not be loaded.");
				}
			}
		}

		this.inputs = inputs;
		this.inputReaders = inputReaders;
		this.inputSerializers = inputSerializers;
		this.inputComparators = inputComparators;
		this.secondarySortComparators = secondarySortComparators;
	}

	/**
	 * Gets the default behavior that readers should use on interrupts.
	 * 
	 * @return The default behavior that readers should use on interrupts.
	 */
	protected ReaderInterruptionBehavior readerInterruptionBehavior() {
		return ReaderInterruptionBehaviors.EXCEPTION_ON_INTERRUPT;
	}
	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategies for each writer.
	 */
	protected void initOutputs() throws Exception
	{
		this.chainedTasks = new ArrayList<ChainedDriver<?, ?>>();
		this.eventualOutputs = new ArrayList<AbstractRecordWriter<?>>();
		this.output = initOutputs(this, this.userCodeClassLoader, this.config, this.chainedTasks, this.eventualOutputs);
	}

	// --------------------------------------------------------------------------------------------
	//                                   Task Context Signature
	// -------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getTaskConfig()
	 */
	@Override
	public TaskConfig getTaskConfig() {
		return this.config;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getUserCodeClassLoader()
	 */
	@Override
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getMemoryManager()
	 */
	@Override
	public MemoryManager getMemoryManager() {
		return getEnvironment().getMemoryManager();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getIOManager()
	 */
	@Override
	public IOManager getIOManager() {
		return getEnvironment().getIOManager();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getStub()
	 */
	@Override
	public S getStub() {
		return this.stub;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getOutputCollector()
	 */
	@Override
	public Collector<OT> getOutputCollector() {
		return this.output;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getOwningNepheleTask()
	 */
	@Override
	public AbstractInvokable getOwningNepheleTask() {
		return this;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#formatLogString(java.lang.String)
	 */
	@Override
	public String formatLogString(String message) {
		return constructLogString(message, getEnvironment().getTaskName(), this);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getInput(int)
	 */
	@Override
	public <X> MutableObjectIterator<X> getInput(int index)
	{
		if (index < 0 || index > this.driver.getNumberOfInputs()) {
			throw new IndexOutOfBoundsException();
		}

		@SuppressWarnings("unchecked")
		final MutableObjectIterator<X> in = (MutableObjectIterator<X>) this.inputs[index];
		return in;
	}
	
	/**
	 * @param <X>
	 * @param index
	 * @return
	 */
	public <X extends Record> MutableReader<X> getReader(int index) {
		if (index < 0 || index > this.driver.getNumberOfInputs()) {
			throw new IndexOutOfBoundsException();
		}

		@SuppressWarnings("unchecked")
		final MutableReader<X> in = (MutableReader<X>) this.inputReaders[index];
		return in;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getInputSerializer(int)
	 */
	@Override
	public <X> TypeSerializer<X> getInputSerializer(int index)
	{
		if (index < 0 || index >= this.driver.getNumberOfInputs()) {
			throw new IndexOutOfBoundsException();
		}

		@SuppressWarnings("unchecked")
		final TypeSerializer<X> serializer = (TypeSerializer<X>) this.inputSerializers[index];
		return serializer;
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getInputComparator(int)
	 */
	@Override
	public <X> TypeComparator<X> getInputComparator(int index)
	{
		if (this.inputComparators == null) {
			throw new IllegalStateException("Comparators have not been created!");
		}
		else if (index < 0 || index >= this.driver.getNumberOfInputs()) {
			throw new IndexOutOfBoundsException();
		}

		@SuppressWarnings("unchecked")
		final TypeComparator<X> comparator = (TypeComparator<X>) this.inputComparators[index];
		return comparator;
	}
	
	@SuppressWarnings("unchecked")
	public <X> TypeComparator<X> getSecondarySortComparator(int index)
	{
		if (this.secondarySortComparators == null) {
			throw new IllegalStateException("Comparators have not been created!");
		}
		else if (index < 0 || index >= this.driver.getNumberOfInputs()) {
			throw new IndexOutOfBoundsException();
		}
		return (TypeComparator<X>) this.secondarySortComparators[index];
	}

	// ============================================================================================
	//                                     Static Utilities
	//
	//            Utilities are consolidated here to ensure a uniform way of running,
	//                   logging, exception handling, and error messages.
	// ============================================================================================

	// --------------------------------------------------------------------------------------------
	//                                       Logging
	// --------------------------------------------------------------------------------------------
	/**
	 * Utility function that composes a string for logging purposes. The string includes the given message,
	 * the given name of the task and the index in its subtask group as well as the number of instances
	 * that exist in its subtask group.
	 *
	 * @param message The main message for the log.
	 * @param taskName The name of the task.
	 * @param parent The nephele task that contains the code producing the message.
	 *
	 * @return The string for logging.
	 */
	public static String constructLogString(String message, String taskName, AbstractInvokable parent)
	{
		final StringBuilder bld = new StringBuilder(128);
		bld.append(message);
		bld.append(':').append(' ');
		bld.append(taskName);
		bld.append(' ').append('(');
		bld.append(parent.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(parent.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
	}

	/**
	 * Prints an error message and throws the given exception. If the exception is of the type
	 * {@link ExceptionInChainedStubException} then the chain of contained exceptions is followed
	 * until an exception of a different type is found.
	 *
	 * @param ex The exception to be thrown.
	 * @param parent The parent task, whose information is included in the log message.
	 * @throws Exception Always thrown.
	 */
	public static void logAndThrowException(Exception ex, AbstractInvokable parent) throws Exception
	{
		String taskName;
		if (ex instanceof ExceptionInChainedStubException) {
			do {
				ExceptionInChainedStubException cex = (ExceptionInChainedStubException) ex;
				taskName = cex.getTaskName();
				ex = cex.getWrappedException();
			} while (ex instanceof ExceptionInChainedStubException);
		} else {
			taskName = parent.getEnvironment().getTaskName();
		}

		if (LOG.isErrorEnabled()) {
			LOG.error(constructLogString("Error in PACT code", taskName, parent));
			LOG.error(ex, ex);
		}

		throw ex;
	}

	// --------------------------------------------------------------------------------------------
	//                             Result Shipping and Chained Tasks
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates the {@link Collector} for the given task, as described by the given configuration. The
	 * output collector contains the writers that forward the data to the different tasks that the given task
	 * is connected to. Each writer applies a the partitioning as described in the configuration.
	 *
	 * @param task The task that the output collector is created for.
	 * @param config The configuration describing the output shipping strategies.
	 * @param cl The classloader used to load user defined types.
	 * @param numOutputs The number of outputs described in the configuration.
	 *
	 * @return The OutputCollector that data produced in this task is submitted to.
	 */
	public static <T> Collector<T> getOutputCollector(AbstractInvokable task, TaskConfig config, ClassLoader cl, List<AbstractRecordWriter<?>> eventualOutputs, int numOutputs)
	throws Exception
	{
		// get the factory for the serializer
		final Class<? extends TypeSerializerFactory<T>> serializerFactoryClass;
		try {
			serializerFactoryClass = config.getSerializerFactoryForOutput(cl);
		} catch (ClassNotFoundException cnfex) {
			throw new Exception("The class registered as output serializer factory could not be loaded.", cnfex);
		}
		final TypeSerializerFactory<T> serializerFactory;

		if (serializerFactoryClass == null) {
			@SuppressWarnings("unchecked")
			TypeSerializerFactory<T> pf = (TypeSerializerFactory<T>) PactRecordSerializerFactory.get();
			serializerFactory = pf;
		} else {
			serializerFactory = InstantiationUtil.instantiate(serializerFactoryClass, TypeSerializerFactory.class);
		}

		// special case the PactRecord
		if (serializerFactory.getDataType().equals(PactRecord.class))
		{
			Preconditions.checkArgument(numOutputs > 0, "must have at least one output");
			final List<AbstractRecordWriter<PactRecord>> writers = new ArrayList<AbstractRecordWriter<PactRecord>>(numOutputs);

			// create a writer for each output
			for (int i = 0; i < numOutputs; i++)
			{
				// create the OutputEmitter from output ship strategy
				final ShipStrategyType strategy = config.getOutputShipStrategy(i);
				final Class<? extends TypeComparatorFactory<PactRecord>> comparatorFactoryClass;
				try {
					comparatorFactoryClass = config.getComparatorFactoryForOutput(i, cl);
				} catch (ClassNotFoundException cnfex) {
					throw new Exception("The class registered as comparator factory for output " + i +
																				" could not be loaded.", cnfex);
				}

				final PactRecordOutputEmitter oe;
				if (comparatorFactoryClass == null) {
					oe = new PactRecordOutputEmitter(strategy);
				} else {
					try {
						final PactRecordComparator comparator = PactRecordComparatorFactory.get().createComparator(
												config.getConfigForOutputParameters(i), cl);
						final DataDistribution distribution = config.getOutputDataDistribution(cl);
						oe = new PactRecordOutputEmitter(strategy, comparator, distribution);
					} catch (ClassNotFoundException cnfex) {
						throw new Exception("The comparator for output " + i +
									" could not be created, because it could not load dependent classes.", cnfex);
					}

				}

				if (strategy == ShipStrategyType.BROADCAST) {
					if (task instanceof AbstractTask) {
						writers.add(new BroadcastRecordWriter<PactRecord>((AbstractTask) task, PactRecord.class));
					} else if (task instanceof AbstractInputTask<?>) {
						writers.add(new BroadcastRecordWriter<PactRecord>((AbstractInputTask<?>) task, PactRecord.class));
					}
				} else {
					if (task instanceof AbstractTask) {
						writers.add(new RecordWriter<PactRecord>((AbstractTask) task, PactRecord.class, oe));
					} else if (task instanceof AbstractInputTask<?>) {
						writers.add(new RecordWriter<PactRecord>((AbstractInputTask<?>) task, PactRecord.class, oe));
					}
				}
			}
			if (eventualOutputs != null) {
				eventualOutputs.addAll(writers);
			}

			@SuppressWarnings("unchecked")
			final Collector<T> outColl = (Collector<T>) new PactRecordOutputCollector(writers);
			return outColl;
		}
		else {
			// generic case
			final List<AbstractRecordWriter<SerializationDelegate<T>>> writers = new ArrayList<AbstractRecordWriter<SerializationDelegate<T>>>(numOutputs);
			@SuppressWarnings("unchecked") // uncritical, simply due to broken generics
			final Class<SerializationDelegate<T>> delegateClazz = (Class<SerializationDelegate<T>>) (Class<?>) SerializationDelegate.class;

			// create a writer for each output
			for (int i = 0; i < numOutputs; i++)
			{
				// create the OutputEmitter from output ship strategy
				final ShipStrategyType strategy = config.getOutputShipStrategy(i);
				final Class<? extends TypeComparatorFactory<T>> comparatorFactoryClass;
				try {
					comparatorFactoryClass = config.getComparatorFactoryForOutput(i, cl);
				} catch (ClassNotFoundException cnfex) {
					throw new Exception("The class registered as comparator factory for output " + i +
																				" could not be loaded.", cnfex);
				}

				final ChannelSelector<SerializationDelegate<T>> oe;
				if (comparatorFactoryClass == null) {
					oe = new OutputEmitter<T>(strategy);
				} else {
					final TypeComparatorFactory<T> compFactory = InstantiationUtil.instantiate(comparatorFactoryClass, TypeComparatorFactory.class);
					try {
						final TypeComparator<T> comparator = compFactory.createComparator(config.getConfigForOutputParameters(i), cl);

						oe = new OutputEmitter<T>(strategy, comparator);
					} catch (ClassNotFoundException cnfex) {
						throw new Exception("The comparator for output " + i +
									" could not be created, because it could not load dependent classes.", cnfex);
					}
				}

				if (strategy == ShipStrategyType.BROADCAST) {
					if (task instanceof AbstractTask) {
						writers.add(new BroadcastRecordWriter<SerializationDelegate<T>>((AbstractTask) task, delegateClazz));
					} else if (task instanceof AbstractInputTask<?>) {
						writers.add(new BroadcastRecordWriter<SerializationDelegate<T>>((AbstractInputTask<?>) task, delegateClazz));
					}
				} else {
					if (task instanceof AbstractTask) {
						writers.add(new RecordWriter<SerializationDelegate<T>>((AbstractTask) task, delegateClazz, oe));
					} else if (task instanceof AbstractInputTask<?>) {
						writers.add(new RecordWriter<SerializationDelegate<T>>((AbstractInputTask<?>) task, delegateClazz, oe));
					}
				}
			}
			if (eventualOutputs != null) {
				eventualOutputs.addAll(writers);
			}
			return new OutputCollector<T>(writers, serializerFactory.getSerializer());
		}
	}

	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategy.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Collector<T> initOutputs(AbstractInvokable nepheleTask, ClassLoader cl, TaskConfig config,
					List<ChainedDriver<?, ?>> chainedTasksTarget, List<AbstractRecordWriter<?>> eventualOutputs)
	throws Exception
	{
		final int numOutputs = config.getNumOutputs();

		// check whether we got any chained tasks
		final int numChained = config.getNumberOfChainedStubs();
		if (numChained > 0)
		{
			// got chained stubs. that means that this one may only have a single forward connection
			if (numOutputs != 1 || config.getOutputShipStrategy(0) != ShipStrategyType.FORWARD) {
				throw new RuntimeException("Plan Generation Bug: Found a chained stub that is not connected via an only forward connection.");
			}

			// instantiate each task
			@SuppressWarnings("rawtypes")
			Collector previous = null;
			for (int i = numChained - 1; i >= 0; --i)
			{
				// get the task first
				final ChainedDriver<?, ?> ct;
				try {
					Class<? extends ChainedDriver<?, ?>> ctc = (Class<? extends ChainedDriver<?, ?>>) config.getChainedTask(i);
					ct = ctc.newInstance();
				}
				catch (Exception ex) {
					throw new RuntimeException("Could not instantiate chained task driver.", ex);
				}

				// get the configuration for the task
				final TaskConfig chainedStubConf = config.getChainedStubConfig(i);
				final String taskName = config.getChainedTaskName(i);

				if (i == numChained -1) {
					// last in chain, instantiate the output collector for this task
					previous = getOutputCollector(nepheleTask, chainedStubConf, cl, eventualOutputs, chainedStubConf.getNumOutputs());
				}

				ct.setup(chainedStubConf, taskName, nepheleTask, cl, previous);
				chainedTasksTarget.add(0, ct);

				previous = ct;
			}
			// the collector of the first in the chain is the collector for the nephele task
			return (Collector<T>) previous;
		}
		// else

		// instantiate the output collector the default way from this configuration
		return getOutputCollector(nepheleTask , config, cl, eventualOutputs, numOutputs);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  User Code LifeCycle
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Opens the given stub using its {@link Stub#open(Configuration)} method. If the open call produces
	 * an exception, a new exception with a standard error message is created, using the encountered exception
	 * as its cause.
	 * 
	 * @param stub The user code instance to be opened.
	 * @param parameters The parameters supplied to the user code.
	 * 
	 * @throws Exception Thrown, if the user code's open method produces an exception.
	 */
	public static void openUserCode(Stub stub, Configuration parameters) throws Exception
	{
		try {
			stub.open(parameters);
		}
		catch (Throwable t) {
			throw new Exception("The user defined 'open(Configuration)' method caused an exception: " + t.getMessage(), t);
		}
	}
	
	/**
	 * Closes the given stub using its {@link Stub#close()} method. If the close call produces
	 * an exception, a new exception with a standard error message is created, using the encountered exception
	 * as its cause.
	 * 
	 * @param stub The user code instance to be closed.
	 * 
	 * @throws Exception Thrown, if the user code's close method produces an exception.
	 */
	public static void closeUserCode(Stub stub) throws Exception
	{
		try {
			stub.close();
		}
		catch (Throwable t) {
			throw new Exception("The user defined 'close()' method caused an exception: " + t.getMessage(), t);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                               Chained Task LifeCycle
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Opens all chained tasks, in the order as they are stored in the array. The opening process
	 * creates a standardized log info message.
	 * 
	 * @param tasks The tasks to be opened.
	 * @param parent The parent task, used to obtain parameters to include in the log message.
	 * @throws Exception Thrown, if the opening encounters an exception.
	 */
	public static void openChainedTasks(List<ChainedDriver<?, ?>> tasks, AbstractInvokable parent) throws Exception
	{
		// start all chained tasks
		for (int i = 0; i < tasks.size(); i++) {
			final ChainedDriver<?, ?> task = tasks.get(i);
			if (LOG.isInfoEnabled())
				LOG.info(constructLogString("Start PACT code", task.getTaskName(), parent));
			task.openTask();
		}
	}
	
	/**
	 * Closes all chained tasks, in the order as they are stored in the array. The closing process
	 * creates a standardized log info message.
	 * 
	 * @param tasks The tasks to be closed.
	 * @param parent The parent task, used to obtain parameters to include in the log message.
	 * @throws Exception Thrown, if the closing encounters an exception.
	 */
	public static void closeChainedTasks(List<ChainedDriver<?, ?>> tasks, AbstractInvokable parent) throws Exception
	{
		for (int i = 0; i < tasks.size(); i++) {
			final ChainedDriver<?, ?> task = tasks.get(i);
			task.closeTask();
			
			if (LOG.isInfoEnabled())
				LOG.info(constructLogString("Finished PACT code", task.getTaskName(), parent));
			
		}
	}
	
	/**
	 * Cancels all tasks via their {@link ChainedDriver#cancelTask()} method. Any occurring exception
	 * and error is suppressed, such that the canceling method of every task is invoked in all cases.
	 * 
	 * @param tasks The tasks to be canceled.
	 */
	public static void cancelChainedTasks(List<ChainedDriver<?, ?>> tasks)
	{
		for (int i = 0; i < tasks.size(); i++) {
			try {
				tasks.get(i).cancelTask();
			} catch (Throwable t) {}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Miscellaneous Utilities
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Instantiates a user code class from is definition in the task configuration.
	 * The class is instantiated without arguments using the null-ary constructor. Instantiation
	 * will fail if this constructor does not exist or is not public.
	 * 
	 * @param <T> The generic type of the user code class.
	 * @param config The task configuration containing the class description.
	 * @param cl The class loader to be used to load the class.
	 * @param superClass The super class that the user code class extends or implements, for type checking.
	 * 
	 * @return An instance of the user code class.
	 */
	public static <T> T instantiateUserCode(TaskConfig config, ClassLoader cl, Class<? super T> superClass)
	{
		// obtain stub implementation class
		try {
			@SuppressWarnings("unchecked")
			final Class<T> clazz = (Class<T>) config.getStubClass(superClass, cl);
			return InstantiationUtil.instantiate(clazz, superClass);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("User Code class was not found in the task configuration.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("User Code class is not a proper subclass of " + superClass.getName(), ccex); 
		}
	}
}