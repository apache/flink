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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.Environment;
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
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.RuntimeContext;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializer;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.shipping.OutputCollector;
import eu.stratosphere.pact.runtime.shipping.OutputEmitter;
import eu.stratosphere.pact.runtime.shipping.PactRecordOutputCollector;
import eu.stratosphere.pact.runtime.shipping.PactRecordOutputEmitter;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.chaining.ExceptionInChainedStubException;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.PactRecordNepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehavior;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.udf.RuntimeUDFContext;

/**
 * The abstract base class for all Pact tasks. Encapsulated common behavior and implements the main life-cycle
 * of the user code.
 */
public class RegularPactTask<S extends Stub, OT> extends AbstractTask implements PactTaskContext<S, OT> {
	
	protected static final Log LOG = LogFactory.getLog(RegularPactTask.class);
	
	private static final boolean USE_BROARDCAST_WRITERS = GlobalConfiguration.getBoolean(
		PactConfigConstants.USE_MULTICAST_FOR_BROADCAST, PactConfigConstants.DEFAULT_USE_MULTICAST_FOR_BROADCAST);
	
	// --------------------------------------------------------------------------------------------

	/**
	 * The driver that invokes the user code (the stub implementation). The central driver in this task
	 * (further drivers may be chained behind this driver).
	 */
	protected volatile PactDriver<S, OT> driver;

	/**
	 * The instantiated user code of this task's main driver.
	 */
	protected S stub;
	
	/**
	 * The collector that forwards the user code's results. May forward to a channel or to chained drivers within
	 * this task.
	 */
	protected Collector<OT> output;

	/**
	 * The output writers for the data that this task forwards to the next task. The latest driver (the central, if no chained
	 * drivers exist, otherwise the last chained driver) produces its output to these writers.
	 */
	protected List<AbstractRecordWriter<?>> eventualOutputs;
	
	/**
	 * The input readers to this task.
	 */
	protected MutableReader<?>[] inputReaders;
	
	/**
	 * The inputs reader, wrapped in an iterator. Prior to the local strategies, etc...
	 */
	protected MutableObjectIterator<?>[] inputIterators;
	
	/**
	 * The local strategies that are applied on the inputs.
	 */
	protected volatile CloseableInputProvider<?>[] localStrategies;
	
	/**
	 * The optional temp barriers on the inputs for dead-lock breaking. Are
	 * optionally resettable.
	 */
	protected volatile TempBarrier<?>[] tempBarriers;
	
	/**
	 * The resettable inputs in the case where no temp barrier is needed.
	 */
	protected volatile SpillingResettableMutableObjectIterator<?>[] resettableInputs;
	
	/**
	 * The inputs to the driver. Return the readers' data after the application of the local strategy
	 * and the temp-table barrier.
	 */
	protected MutableObjectIterator<?>[] inputs;

	/**
	 * The serializers for the input data type.
	 */
	protected TypeSerializer<?>[] inputSerializers;

	/**
	 * The comparators for the central driver.
	 */
	protected TypeComparator<?>[] inputComparators;

	/**
	 * The task configuration with the setup parameters.
	 */
	protected TaskConfig config;

	/**
	 * The class loader used to instantiate user code and user data types.
	 */
	protected ClassLoader userCodeClassLoader;

	/**
	 * A list of chained drivers, if there are any.
	 */
	protected ArrayList<ChainedDriver<?, ?>> chainedTasks;
	
	/**
	 * Certain inputs may be excluded from resetting. For example, the initial partial solution
	 * in an iteration head must not be reseted (it is read through the back channel), when all
	 * others are reseted.
	 */
	private boolean[] excludeFromReset;
	
	/**
	 * Flag indicating for each input whether it is cached and can be reseted.
	 */
	private boolean[] inputIsCached;
			
	/**
	 * flag indicating for each input whether it must be asynchronously materialized.
	 */
	private boolean[] inputIsAsyncMaterialized;
	
	/**
	 * The amount of memory per input that is dedicated to the materialization.
	 */
	private long[] materializationMemory;

	/**
	 * The flag that tags the task as still running. Checked periodically to abort processing.
	 */
	protected volatile boolean running = true;

	// --------------------------------------------------------------------------------------------
	//                                  Nephele Task Interface
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#registerInputOutput()
	 */
	@Override
	public void registerInputOutput() {
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

		// initialize the readers. this is necessary for nephele to create the input gates
		// however, this does not trigger any local processing.
		try {
			initInputReaders();
		} catch (Exception e) {
			throw new RuntimeException("Initializing the input streams failed" +
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
		}

		// initialize the writers. this is necessary for nephele to create the output gates.
		// because in the presence of chained tasks, the tasks writers depend on the last task in the chain,
		// we need to initialize the chained tasks as well. the chained tasks are only set up, but no work
		// (such as setting up a sorter, etc.) starts
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
	public void invoke() throws Exception {
		
		if (LOG.isInfoEnabled())
			LOG.info(formatLogString("Start PACT code."));
		
		// sanity check the input setup
		final int numInputs = this.driver.getNumberOfInputs();
		// whatever happens in this scope, make sure that the local strategies are cleaned up!
		// note that the initialization of the local strategies is in the try-finally block as well,
		// so that the thread that creates them catches its own errors that may happen in that process.
		// this is especially important, since there may be asynchronous closes (such as through canceling).
		try {
			// initialize the remaining data structures on the input and trigger the local processing
			// the local processing includes building the dams / caches
			try {
				initInputsSerializersAndComparators(numInputs);
				initLocalStrategies(numInputs);
			} catch (Exception e) {
				throw new RuntimeException("Initializing the input processing failed" +
					e.getMessage() == null ? "." : ": " + e.getMessage(), e);
			}
			
			if (!this.running) {
				if (LOG.isDebugEnabled())
					LOG.info(formatLogString("Task cancelled before PACT code was started."));
				return;
			}
			
			// pre main-function initialization
			initialize();
	
			// the work goes here
			run();
		}
		finally {
			// clean up in any case!
			closeLocalStrategiesAndCaches();
		}
		
		if (this.running) {
			if (LOG.isInfoEnabled())
				LOG.info(formatLogString("Finished PACT code."));
		} else {
			if (LOG.isWarnEnabled())
				LOG.warn(formatLogString("PACT code cancelled."));
		}
	}
	
	protected void initialize() throws Exception {
		try {
			this.driver.setup(this);
		}
		catch (Throwable t) {
			throw new Exception("The pact driver setup for '" + this.getEnvironment().getTaskName() +
				"' , caused an error: " + t.getMessage(), t);
		}
		
		try {
			final Class<? super S> userCodeFunctionType = this.driver.getStubType();
			// if the class is null, the driver has no user code 
			if (userCodeFunctionType != null) {
				this.stub = initStub(userCodeFunctionType);
				this.stub.setRuntimeContext(getRuntimeContext(getEnvironment().getTaskName()));
				
			}
		} catch (Exception e) {
			throw new RuntimeException("Initializing the user code and the configuration failed" +
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
		}
	}
	
	protected void run() throws Exception {
		// ---------------------------- Now, the actual processing starts ------------------------
		// check for asynchronous canceling
		if (!this.running) {
			return;
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
			if (this.stub != null) {
				try {
					Configuration stubConfig = this.config.getStubParameters();
					this.stub.open(stubConfig);
					stubOpen = true;
				}
				catch (Throwable t) {
					throw new Exception("The user defined 'open()' method caused an exception: " + t.getMessage(), t);
				}
			}

			// run the user code
			this.driver.run();

			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (this.running && this.stub != null) {
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
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception {
		this.running = false;
		if (LOG.isWarnEnabled())
			LOG.warn(formatLogString("Cancelling PACT code"));
		
		closeLocalStrategiesAndCaches();
		
		if (this.driver != null) {
			this.driver.cancel();
		}
	}
	
	protected void closeLocalStrategiesAndCaches() {
		if (this.localStrategies != null) {
			for (int i = 0; i < this.localStrategies.length; i++) {
				if (this.localStrategies[i] != null) {
					try {
						this.localStrategies[i].close();
					} catch (Throwable t) {
						LOG.error("Error closing local strategy for input " + i, t);
					}
				}
			}
		}
		if (this.tempBarriers != null) {
			for (int i = 0; i < this.tempBarriers.length; i++) {
				if (this.tempBarriers[i] != null) {
					try {
						this.tempBarriers[i].close();
					} catch (Throwable t) {
						LOG.error("Error closing temp barrier for input " + i, t);
					}
				}
			}
		}
		if (this.resettableInputs != null) {
			for (int i = 0; i < this.resettableInputs.length; i++) {
				if (this.resettableInputs[i] != null) {
					try {
						this.resettableInputs[i].close();
					} catch (Throwable t) {
						LOG.error("Error closing cache for input " + i, t);
					}
				}
			}
		}
	}

	/**
	 * Sets the class-loader to be used to load the user code.
	 *
	 * @param cl The class-loader to be used to load the user code.
	 */
	public void setUserCodeClassLoader(ClassLoader cl) {
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
	protected S initStub(Class<? super S> stubSuperClass) throws Exception {
		// obtain stub implementation class
		try {
			@SuppressWarnings("unchecked")
			Class<S> stubClass = (Class<S>) this.config.getStubClass(stubSuperClass, this.userCodeClassLoader);
			return InstantiationUtil.instantiate(stubClass, stubSuperClass);
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
	protected void initInputReaders() throws Exception {
		final int numInputs = this.driver.getNumberOfInputs();
		final MutableReader<?>[] inputReaders = new MutableReader[numInputs];
		
		int numGates = 0;
		
		for (int i = 0; i < numInputs; i++) {
			//  ---------------- create the input readers ---------------------
			// in case where a logical input unions multiple physical inputs, create a union reader
			final int groupSize = this.config.getGroupSize(i);
			numGates += groupSize;
			if (groupSize == 1) {
				// non-union case
				inputReaders[i] = new MutableRecordReader<Record>(this);
			} else if (groupSize > 1){
				// union case
				@SuppressWarnings("unchecked")
				MutableRecordReader<Record>[] readers = new MutableRecordReader[groupSize];
				for (int j = 0; j < groupSize; ++j) {
					readers[j] = new MutableRecordReader<Record>(this);
				}
				inputReaders[i] = new MutableUnionRecordReader<Record>(readers);
			} else {
				throw new Exception("Illegal input group size in task configuration: " + groupSize);
			}
		}
		this.inputReaders = inputReaders;
		
		// final sanity check
		if (numGates != this.config.getNumInputs()) {
			throw new Exception("Illegal configuration: Number of input gates and group sizes are not consistent.");
		}
	}
	
	/**
	 * Creates all the serializers and comparators.
	 */
	protected void initInputsSerializersAndComparators(int numInputs) throws Exception {
		this.inputSerializers = new TypeSerializer[numInputs];
		this.inputComparators = this.driver.requiresComparatorOnInput() ? new TypeComparator[numInputs] : null;
		this.inputIterators = new MutableObjectIterator[numInputs];

		for (int i = 0; i < numInputs; i++) {
			//  ---------------- create the serializer first ---------------------
			final TypeSerializerFactory<?> serializerFactory = this.config.getInputSerializer(i, this.userCodeClassLoader);
			this.inputSerializers[i] = serializerFactory.getSerializer();
			
			//  ---------------- create the driver's comparator ---------------------
			if (this.inputComparators != null) {
				final TypeComparatorFactory<?> comparatorFactory = this.config.getDriverComparator(i, this.userCodeClassLoader);
				this.inputComparators[i] = comparatorFactory.createComparator();
			}
			
			this.inputIterators[i] = createInputIterator(i, this.inputReaders[i], this.inputSerializers[i]);
		}
	}
	
	/**
	 * 
	 * NOTE: This method must be invoked after the invocation of {@code #initInputReaders()} and
	 * {@code #initInputSerializersAndComparators(int)}!
	 * 
	 * @param numInputs
	 */
	protected void initLocalStrategies(int numInputs) throws Exception {
		
		final MemoryManager memMan = getMemoryManager();
		final IOManager ioMan = getIOManager();
		
		this.localStrategies = new CloseableInputProvider[numInputs];
		this.inputs = new MutableObjectIterator[numInputs];
		this.excludeFromReset = new boolean[numInputs];
		this.inputIsCached = new boolean[numInputs];
		this.inputIsAsyncMaterialized = new boolean[numInputs];
		this.materializationMemory = new long[numInputs];
		
		// set up the local strategies first, such that the can work before any temp barrier is created
		for (int i = 0; i < numInputs; i++) {
			initInputLocalStrategy(i);
		}
		
		// we do another loop over the inputs, because we want to instantiate all
		// sorters, etc before requesting the first input (as this call may block)
		
		// we have two types of materialized inputs, and both are replayable (can act as a cache)
		// The first variant materializes in a different thread and hence
		// acts as a pipeline breaker. this one should only be there, if a pipeline breaker is needed.
		// the second variant spills to the side and will not read unless the result is also consumed
		// in a pipelined fashion.
		this.resettableInputs = new SpillingResettableMutableObjectIterator[numInputs];
		this.tempBarriers = new TempBarrier[numInputs];
		
		for (int i = 0; i < numInputs; i++) {
			final long memory;
			final boolean async = this.config.isInputAsynchronouslyMaterialized(i);
			final boolean cached =  this.config.isInputCached(i);
			
			this.inputIsAsyncMaterialized[i] = async;
			this.inputIsCached[i] = cached;
			
			if (async || cached) {
				memory = this.config.getInputMaterializationMemory(i);
				this.materializationMemory[i] = memory;
			} else {
				memory = 0;
			}
			
			if (async) {
				final int pages = memMan.computeNumberOfPages(memory);
				@SuppressWarnings({ "unchecked", "rawtypes" })
				TempBarrier<?> barrier = new TempBarrier(this, getInput(i), this.inputSerializers[i], memMan, ioMan, pages);
				barrier.startReading();
				this.tempBarriers[i] = barrier;
				this.inputs[i] = null;
			} else if (cached) {
				@SuppressWarnings({ "unchecked", "rawtypes" })
				SpillingResettableMutableObjectIterator<?> iter = new SpillingResettableMutableObjectIterator(
					getInput(i), this.inputSerializers[i], getMemoryManager(), getIOManager(), memory, this);
				this.resettableInputs[i] = iter;
				this.inputs[i] = iter;
			}
		}
	}
	
	protected void resetAllInputs() throws Exception {
		// close all local-strategies. they will either get re-initialized, or we have
		// read them now and their data is cached
		for (int i = 0; i < this.localStrategies.length; i++) {
			if (this.localStrategies[i] != null) {
				this.localStrategies[i].close();
				this.localStrategies[i] = null;
			}
		}
		
		final MemoryManager memMan = getMemoryManager();
		final IOManager ioMan = getIOManager();
		
		// reset the caches, or re-run the input local strategy
		for (int i = 0; i < this.inputs.length; i++) {
			if (this.excludeFromReset[i]) {
				if (this.tempBarriers[i] != null) {
					this.tempBarriers[i].close();
					this.tempBarriers[i] = null;
				} else if (this.resettableInputs[i] != null) {
					this.resettableInputs[i].close();
					this.resettableInputs[i] = null;
				}
			} else {
				// make sure the input is not available directly, but are lazily fetched again
				this.inputs[i] = null;
				
				if (this.inputIsCached[i]) {
					if (this.tempBarriers[i] != null) {
						this.inputs[i] = this.tempBarriers[i].getIterator();
					} else if (this.resettableInputs[i] != null) {
						this.resettableInputs[i].reset();
						this.inputs[i] = this.resettableInputs[i];
					} else {
						throw new RuntimeException("Found a resettable input, but no temp barrier and no resettable iterator.");
					}
				} else {
					// close the async barrier if there is one
					if (this.tempBarriers[i] != null) {
						this.tempBarriers[i].close();
					}
					
					// recreate the local strategy
					initInputLocalStrategy(i);
					
					if (this.inputIsAsyncMaterialized[i]) {
						final long memory = this.materializationMemory[i];
						final int pages = memMan.computeNumberOfPages(memory);
						@SuppressWarnings({ "unchecked", "rawtypes" })
						TempBarrier<?> barrier = new TempBarrier(this, getInput(i), this.inputSerializers[i], memMan, ioMan, pages);
						barrier.startReading();
						this.tempBarriers[i] = barrier;
						this.inputs[i] = null;
					}
				}
			}
		}
	}
	
	protected void excludeFromReset(int inputNum) {
		this.excludeFromReset[inputNum] = true;
	}
	
	private void initInputLocalStrategy(int inputNum) throws Exception {
		// check if there is already a strategy
		if (this.localStrategies[inputNum] != null) {
			throw new IllegalStateException();
		}
		
		// now set up the local strategy
		final LocalStrategy localStrategy = this.config.getInputLocalStrategy(inputNum);
		if (localStrategy != null) {
			switch (localStrategy) {
			case NONE:
				// the input is as it is
				this.inputs[inputNum] = this.inputIterators[inputNum];
				break;
			case SORT:
				@SuppressWarnings({ "rawtypes", "unchecked" })
				UnilateralSortMerger<?> sorter = new UnilateralSortMerger(getMemoryManager(), getIOManager(),
					this.inputIterators[inputNum], this, this.inputSerializers[inputNum], getLocalStrategyComparator(inputNum),
					this.config.getMemoryInput(inputNum), this.config.getFilehandlesInput(inputNum),
					this.config.getSpillingThresholdInput(inputNum));
				// set the input to null such that it will be lazily fetched from the input strategy
				this.inputs[inputNum] = null;
				this.localStrategies[inputNum] = sorter;
				break;
			case COMBININGSORT:
				// sanity check this special case!
				// this still breaks a bit of the abstraction!
				// we should have nested configurations for the local strategies to solve that
				if (inputNum != 0) {
					throw new IllegalStateException("Performing combining sort outside a reduce task!");
				}
				
				// instantiate ourselves a combiner. we should not use the stub, because the sort and the
				// subsequent reduce would otherwise share it multithreaded
				final S localStub;
				try {
					final Class<S> userCodeFunctionType = this.driver.getStubType();
					// if the class is null, the driver has no user code 
					if (userCodeFunctionType != null && GenericReducer.class.isAssignableFrom(userCodeFunctionType)) {
						localStub = initStub(userCodeFunctionType);
					} else {
						throw new IllegalStateException("Performing combining sort outside a reduce task!");
					}
				} catch (Exception e) {
					throw new RuntimeException("Initializing the user code and the configuration failed" +
						e.getMessage() == null ? "." : ": " + e.getMessage(), e);
				}

				@SuppressWarnings({ "rawtypes", "unchecked" })
				CombiningUnilateralSortMerger<?> cSorter = new CombiningUnilateralSortMerger(
					(GenericReducer) localStub, getMemoryManager(), getIOManager(), this.inputIterators[inputNum], 
					this, this.inputSerializers[inputNum], getLocalStrategyComparator(inputNum),
					this.config.getMemoryInput(inputNum), this.config.getFilehandlesInput(inputNum),
					this.config.getSpillingThresholdInput(inputNum), false);
				// set the input to null such that it will be lazily fetched from the input strategy
				this.inputs[inputNum] = null;
				this.localStrategies[inputNum] = cSorter;
				break;
			default:
				throw new Exception("Unrecognized local strategy provided: " + localStrategy.name());
			}
		} else {
			// no local strategy in the config
			this.inputs[inputNum] = this.inputIterators[inputNum];
		}
	}
	
	private <T> TypeComparator<T> getLocalStrategyComparator(int inputNum) throws Exception {
		TypeComparatorFactory<T> compFact = this.config.getInputComparator(inputNum, this.userCodeClassLoader);
		if (compFact == null) {
			throw new Exception("Missing comparator factory for local strategy on input " + inputNum);
		}
		return compFact.createComparator();
	}
	
	protected MutableObjectIterator<?> createInputIterator(int inputIndex, 
		MutableReader<?> inputReader, TypeSerializer<?> serializer)
	{
		if (serializer.getClass() == PactRecordSerializer.class) {
			// pact record specific deserialization
			@SuppressWarnings("unchecked")
			MutableReader<PactRecord> reader = (MutableReader<PactRecord>) inputReader;
			return new PactRecordNepheleReaderIterator(reader, readerInterruptionBehavior(inputIndex));
		} else {
			// generic data type serialization
			@SuppressWarnings("unchecked")
			MutableReader<DeserializationDelegate<?>> reader = (MutableReader<DeserializationDelegate<?>>) inputReader;
			@SuppressWarnings({ "unchecked", "rawtypes" })
			final MutableObjectIterator<?> iter = new NepheleReaderIterator(reader, serializer,
					readerInterruptionBehavior(inputIndex));
			return iter;
		}
	}
	/**
	 * Gets the default behavior that readers should use on interrupts.
	 * 
	 * @param inputGateIndex
	 * @return The default behavior that readers should use on interrupts.
	 */
	protected ReaderInterruptionBehavior readerInterruptionBehavior(int inputGateIndex) {
		return ReaderInterruptionBehaviors.EXCEPTION_ON_INTERRUPT;
	}
	
	public RuntimeContext getRuntimeContext(String taskName) {
		Environment env = getEnvironment();
		return new RuntimeUDFContext(taskName, env.getCurrentNumberOfSubtasks(), env.getIndexInSubtaskGroup());
	}
	
	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategies for each writer.
	 */
	protected void initOutputs() throws Exception {
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
	public <X> MutableObjectIterator<X> getInput(int index) {
		if (index < 0 || index > this.driver.getNumberOfInputs()) {
			throw new IndexOutOfBoundsException();
		}
		
		// check for lazy assignment from input strategies
		if (this.inputs[index] != null) {
			@SuppressWarnings("unchecked")
			MutableObjectIterator<X> in = (MutableObjectIterator<X>) this.inputs[index];
			return in;
		} else {
			final MutableObjectIterator<X> in;
			try {
				if (this.tempBarriers[index] != null) {
					@SuppressWarnings("unchecked")
					MutableObjectIterator<X> iter = (MutableObjectIterator<X>) this.tempBarriers[index].getIterator();
					in = iter;
				} else if (this.localStrategies[index] != null) {
					@SuppressWarnings("unchecked")
					MutableObjectIterator<X> iter = (MutableObjectIterator<X>) this.localStrategies[index].getIterator();
					in = iter;
				} else {
					throw new RuntimeException("Bug: null input iterator, null temp barrier, and null local strategy.");
				}
				this.inputs[index] = in;
				return in;
			} catch (InterruptedException iex) {
				throw new RuntimeException("Interrupted while waiting for input " + index + " to become available.");
			} catch (IOException ioex) {
				throw new RuntimeException("An I/O Exception occurred whily obaining input " + index + ".");
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getInputSerializer(int)
	 */
	@Override
	public <X> TypeSerializer<X> getInputSerializer(int index) {
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
	public <X> TypeComparator<X> getInputComparator(int index) {
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
	
	/**
	 * Gets the serializer for the output data type of the main (i.e. the non-chained) driver.
	 * 
	 * @return The serializer for the output data type of the main driver.
	 */
	public <T> TypeSerializer<T> getOutputTypeSerializer() {
		TypeSerializerFactory<T> factory = this.config.getOutputSerializer(this.userCodeClassLoader);
		return factory.getSerializer();
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
		if (numOutputs <= 0) {
			throw new Exception("BUG: The task must have at least one output");
		}
		
		// get the factory for the serializer
		final TypeSerializerFactory<T> serializerFactory = config.getOutputSerializer(cl);

		// special case the PactRecord
		if (serializerFactory.getDataType().equals(PactRecord.class)) {
			final List<AbstractRecordWriter<PactRecord>> writers = new ArrayList<AbstractRecordWriter<PactRecord>>(numOutputs);

			// create a writer for each output
			for (int i = 0; i < numOutputs; i++) {
				// create the OutputEmitter from output ship strategy
				final ShipStrategyType strategy = config.getOutputShipStrategy(i);
				final TypeComparatorFactory<?> compFact = config.getOutputComparator(i, cl);
				final PactRecordOutputEmitter oe;
				if (compFact == null) {
					oe = new PactRecordOutputEmitter(strategy);
				} else {
					if (compFact instanceof PactRecordComparatorFactory) {
						final PactRecordComparator comparator = ((PactRecordComparatorFactory) compFact).createComparator();
						final DataDistribution distribution = config.getOutputDataDistribution(cl);
						oe = new PactRecordOutputEmitter(strategy, comparator, distribution);
					} else {
						throw new Exception("Incompatibe serializer-/comparator factories.");
					}
				}

				if (strategy == ShipStrategyType.BROADCAST && USE_BROARDCAST_WRITERS) {
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
				final TypeComparatorFactory<T> compFactory = config.getOutputComparator(i, cl);

				final ChannelSelector<SerializationDelegate<T>> oe;
				if (compFactory == null) {
					oe = new OutputEmitter<T>(strategy);
				} else {
					final TypeComparator<T> comparator = compFactory.createComparator();
					oe = new OutputEmitter<T>(strategy, comparator);
				}

				if (strategy == ShipStrategyType.BROADCAST && USE_BROARDCAST_WRITERS) {
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
		if (numChained > 0) {
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
	public static void openUserCode(Stub stub, Configuration parameters) throws Exception {
		try {
			stub.open(parameters);
		} catch (Throwable t) {
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
	public static void closeUserCode(Stub stub) throws Exception {
		try {
			stub.close();
		} catch (Throwable t) {
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
	public static void openChainedTasks(List<ChainedDriver<?, ?>> tasks, AbstractInvokable parent) throws Exception {
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
	public static void closeChainedTasks(List<ChainedDriver<?, ?>> tasks, AbstractInvokable parent) throws Exception {
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
	public static void cancelChainedTasks(List<ChainedDriver<?, ?>> tasks) {
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
	public static <T> T instantiateUserCode(TaskConfig config, ClassLoader cl, Class<? super T> superClass) {
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