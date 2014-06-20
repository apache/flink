/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.task;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.api.common.distributions.DataDistribution;
import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.functions.GenericCombine;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.execution.CancelTaskException;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.runtime.io.api.ChannelSelector;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.runtime.io.api.MutableReader;
import eu.stratosphere.runtime.io.api.MutableRecordReader;
import eu.stratosphere.runtime.io.api.MutableUnionRecordReader;
import eu.stratosphere.runtime.io.api.BufferWriter;
import eu.stratosphere.nephele.services.accumulators.AccumulatorEvent;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.shipping.OutputCollector;
import eu.stratosphere.pact.runtime.shipping.OutputEmitter;
import eu.stratosphere.pact.runtime.shipping.RecordOutputCollector;
import eu.stratosphere.pact.runtime.shipping.RecordOutputEmitter;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.chaining.ExceptionInChainedStubException;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.ReaderIterator;
import eu.stratosphere.pact.runtime.task.util.RecordReaderIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.udf.RuntimeUDFContext;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.InstantiationUtil;
import eu.stratosphere.util.MutableObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The abstract base class for all tasks. Encapsulated common behavior and implements the main life-cycle
 * of the user code.
 */
public class RegularPactTask<S extends Function, OT> extends AbstractInvokable implements PactTaskContext<S, OT> {

	protected static final Log LOG = LogFactory.getLog(RegularPactTask.class);

	// --------------------------------------------------------------------------------------------

	/**
	 * The driver that invokes the user code (the stub implementation). The central driver in this task
	 * (further drivers may be chained behind this driver).
	 */
	protected volatile PactDriver<S, OT> driver;

	/**
	 * The instantiated user code of this task's main operator (driver). May be null if the operator has no udf.
	 */
	protected S stub;

	/**
	 * The udf's runtime context.
	 */
	protected RuntimeUDFContext runtimeUdfContext;

	/**
	 * The collector that forwards the user code's results. May forward to a channel or to chained drivers within
	 * this task.
	 */
	protected Collector<OT> output;

	/**
	 * The output writers for the data that this task forwards to the next task. The latest driver (the central, if no chained
	 * drivers exist, otherwise the last chained driver) produces its output to these writers.
	 */
	protected List<BufferWriter> eventualOutputs;

	/**
	 * The input readers to this task.
	 */
	protected MutableReader<?>[] inputReaders;

	/**
	 * The input readers for the configured broadcast variables for this task.
	 */
	protected MutableReader<?>[] broadcastInputReaders;
	
	/**
	 * The inputs reader, wrapped in an iterator. Prior to the local strategies, etc...
	 */
	protected MutableObjectIterator<?>[] inputIterators;

	/**
	 * The input readers for the configured broadcast variables, wrapped in an iterator. 
	 * Prior to the local strategies, etc...
	 */
	protected MutableObjectIterator<?>[] broadcastInputIterators;
	
	protected int[] iterativeInputs;
	
	protected int[] iterativeBroadcastInputs;
	
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
	 * The inputs to the operator. Return the readers' data after the application of the local strategy
	 * and the temp-table barrier.
	 */
	protected MutableObjectIterator<?>[] inputs;

	/**
	 * The serializers for the input data type.
	 */
	protected TypeSerializerFactory<?>[] inputSerializers;

	/**
	 * The serializers for the broadcast input data types.
	 */
	protected TypeSerializerFactory<?>[] broadcastInputSerializers;

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
	private int[] materializationMemory;

	/**
	 * The flag that tags the task as still running. Checked periodically to abort processing.
	 */
	protected volatile boolean running = true;

	// --------------------------------------------------------------------------------------------
	//                                  Task Interface
	// --------------------------------------------------------------------------------------------


	/**
	 * Initialization method. Runs in the execution graph setup phase in the JobManager
	 * and as a setup method on the TaskManager.
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
		Configuration taskConf = getTaskConfiguration();
		taskConf.setClassLoader(this.userCodeClassLoader);
		this.config = new TaskConfig(taskConf);

		// now get the operator class which drives the operation
		final Class<? extends PactDriver<S, OT>> driverClass = this.config.getDriver();
		this.driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);

		// initialize the readers. this is necessary for nephele to create the input gates
		// however, this does not trigger any local processing.
		try {
			initInputReaders();
			initBroadcastInputReaders();
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


	/**
	 * The main work method.
	 */
	@Override
	public void invoke() throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug(formatLogString("Start task code."));
		}

		// whatever happens in this scope, make sure that the local strategies are cleaned up!
		// note that the initialization of the local strategies is in the try-finally block as well,
		// so that the thread that creates them catches its own errors that may happen in that process.
		// this is especially important, since there may be asynchronous closes (such as through canceling).
		try {
			// initialize the serializers (one per channel) of the record writers
			initOutputWriters(this.eventualOutputs);

			// initialize the remaining data structures on the input and trigger the local processing
			// the local processing includes building the dams / caches
			try {
				int numInputs = driver.getNumberOfInputs();
				int numBroadcastInputs = this.config.getNumBroadcastInputs();
				
				initInputsSerializersAndComparators(numInputs);
				initBroadcastInputsSerializers(numBroadcastInputs);
				
				// set the iterative status for inputs and broadcast inputs
				{
					List<Integer> iterativeInputs = new ArrayList<Integer>();
					
					for (int i = 0; i < numInputs; i++) {
						final int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeGate(i);
			
						if (numberOfEventsUntilInterrupt < 0) {
							throw new IllegalArgumentException();
						}
						else if (numberOfEventsUntilInterrupt > 0) {
							this.inputReaders[i].setIterative(numberOfEventsUntilInterrupt);
							iterativeInputs.add(i);
				
							if (LOG.isDebugEnabled()) {
								LOG.debug(formatLogString("Input [" + i + "] reads in supersteps with [" +
										+ numberOfEventsUntilInterrupt + "] event(s) till next superstep."));
							}
						}
					}
					this.iterativeInputs = asArray(iterativeInputs);
				}
				
				{
					List<Integer> iterativeBcInputs = new ArrayList<Integer>();
					
					for (int i = 0; i < numBroadcastInputs; i++) {
						final int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeBroadcastGate(i);
						
						if (numberOfEventsUntilInterrupt < 0) {
							throw new IllegalArgumentException();
						}
						else if (numberOfEventsUntilInterrupt > 0) {
							this.broadcastInputReaders[i].setIterative(numberOfEventsUntilInterrupt);
							iterativeBcInputs.add(i);
				
							if (LOG.isDebugEnabled()) {
								LOG.debug(formatLogString("Broadcast input [" + i + "] reads in supersteps with [" +
										+ numberOfEventsUntilInterrupt + "] event(s) till next superstep."));
							}
						}
					}
					this.iterativeBroadcastInputs = asArray(iterativeBcInputs);
				}
				
				initLocalStrategies(numInputs);
			} catch (Exception e) {
				throw new RuntimeException("Initializing the input processing failed" +
					e.getMessage() == null ? "." : ": " + e.getMessage(), e);
			}

			if (!this.running) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(formatLogString("Task cancelled before task code was started."));
				}
				return;
			}

			// pre main-function initialization
			initialize();

			// read the broadcast variables
			for (int i = 0; i < this.config.getNumBroadcastInputs(); i++) {
				final String name = this.config.getBroadcastInputName(i);
				readAndSetBroadcastInput(i, name, this.runtimeUdfContext);
			}

			// the work goes here
			run();
		}
		finally {
			// clean up in any case!
			closeLocalStrategiesAndCaches();
		}

		if (this.running) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(formatLogString("Finished task code."));
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug(formatLogString("Task code cancelled."));
			}
		}
	}

	@Override
	public void cancel() throws Exception {
		this.running = false;

		if (LOG.isDebugEnabled()) {
			LOG.debug(formatLogString("Cancelling task code"));
		}

		try {
			if (this.driver != null) {
				this.driver.cancel();
			}
		} finally {
			closeLocalStrategiesAndCaches();
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
	//                                  Main Work Methods
	// --------------------------------------------------------------------------------------------

	protected void initialize() throws Exception {
		// create the operator
		try {
			this.driver.setup(this);
		}
		catch (Throwable t) {
			throw new Exception("The driver setup for '" + this.getEnvironment().getTaskName() +
				"' , caused an error: " + t.getMessage(), t);
		}
		
		this.runtimeUdfContext = createRuntimeContext(getEnvironment().getTaskName());
		
		// instantiate the UDF
		try {
			final Class<? super S> userCodeFunctionType = this.driver.getStubType();
			// if the class is null, the driver has no user code
			if (userCodeFunctionType != null) {
				this.stub = initStub(userCodeFunctionType);
			}
		} catch (Exception e) {
			throw new RuntimeException("Initializing the UDF" +
				e.getMessage() == null ? "." : ": " + e.getMessage(), e);
		}
	}
	
	protected <X> void readAndSetBroadcastInput(int inputNum, String bcVarName, RuntimeUDFContext context) throws IOException {
		// drain the broadcast inputs

		@SuppressWarnings("unchecked")
		final MutableObjectIterator<X> reader =  (MutableObjectIterator<X>) this.broadcastInputIterators[inputNum];
		
		@SuppressWarnings("unchecked")
		final TypeSerializer<X> serializer =  (TypeSerializer<X>) this.broadcastInputSerializers[inputNum].getSerializer();

		ArrayList<X> collection = new ArrayList<X>();
		
		X record = serializer.createInstance();
		while (this.running && ((record = reader.next(record)) != null)) {
			collection.add(record);
			record = serializer.createInstance();
		}
		context.setBroadcastVariable(bcVarName, collection);
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

			// Collect the accumulators of all involved UDFs and send them to the
			// JobManager. close() has been called earlier for all involved UDFs
			// (using this.stub.close() and closeChainedTasks()), so UDFs can no longer
			// modify accumulators.ll;
			if (this.stub != null) {
				// collect the counters from the stub
				Map<String, Accumulator<?,?>> accumulators = this.stub.getRuntimeContext().getAllAccumulators();
				RegularPactTask.reportAndClearAccumulators(getEnvironment(), accumulators, this.chainedTasks);
			}
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

			ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

			if (ex instanceof CancelTaskException) {
				// forward canceling exception
				throw ex;
			}
			else if (this.running) {
				// throw only if task was not cancelled. in the case of canceling, exceptions are expected 
				RegularPactTask.logAndThrowException(ex, this);
			}
		}
		finally {
			this.driver.cleanup();
		}
	}

	/**
	 * This method is called at the end of a task, receiving the accumulators of
	 * the task and the chained tasks. It merges them into a single map of
	 * accumulators and sends them to the JobManager.
	 *
	 * @param chainedTasks
	 *          Each chained task might have accumulators which will be merged
	 *          with the accumulators of the stub.
	 */
	protected static void reportAndClearAccumulators(Environment env, Map<String, Accumulator<?, ?>> accumulators,
			ArrayList<ChainedDriver<?, ?>> chainedTasks) {

		// We can merge here the accumulators from the stub and the chained
		// tasks. Type conflicts can occur here if counters with same name but
		// different type were used.

		for (ChainedDriver<?, ?> chainedTask : chainedTasks) {
			Map<String, Accumulator<?, ?>> chainedAccumulators = chainedTask.getStub().getRuntimeContext().getAllAccumulators();
			AccumulatorHelper.mergeInto(accumulators, chainedAccumulators);
		}

		// Don't report if the UDF didn't collect any accumulators
		if (accumulators.size() == 0) {
			return;
		}

		// Report accumulators to JobManager
		synchronized (env.getAccumulatorProtocolProxy()) {
			try {
				env.getAccumulatorProtocolProxy().reportAccumulatorResult(
						new AccumulatorEvent(env.getJobID(), accumulators, true));
			} catch (IOException e) {
				throw new RuntimeException("Communication with JobManager is broken. Could not send accumulators.", e);
			}
		}

		// We also clear the accumulators, since stub instances might be reused
		// (e.g. in iterations) and we don't want to count twice. This may not be
		// done before sending
		AccumulatorHelper.resetAndClearAccumulators(accumulators);
		for (ChainedDriver<?, ?> chainedTask : chainedTasks) {
			AccumulatorHelper.resetAndClearAccumulators(chainedTask.getStub().getRuntimeContext().getAllAccumulators());
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

	// --------------------------------------------------------------------------------------------
	//                                 Task Setup and Teardown
	// --------------------------------------------------------------------------------------------

	/**
	 * @return the last output collector in the collector chain
	 */
	@SuppressWarnings("unchecked")
	protected Collector<OT> getLastOutputCollector() {
		int numChained = this.chainedTasks.size();
		return (numChained == 0) ? output : (Collector<OT>) chainedTasks.get(numChained - 1).getOutputCollector();
	}

	/**
	 * Sets the last output {@link Collector} of the collector chain of this {@link RegularPactTask}.
	 * <p/>
	 * In case of chained tasks, the output collector of the last {@link ChainedDriver} is set. Otherwise it is the
	 * single collector of the {@link RegularPactTask}.
	 *
	 * @param newOutputCollector new output collector to set as last collector
	 */
	protected void setLastOutputCollector(Collector<OT> newOutputCollector) {
		int numChained = this.chainedTasks.size();

		if (numChained == 0) {
			output = newOutputCollector;
			return;
		}

		chainedTasks.get(numChained - 1).setOutputCollector(newOutputCollector);
	}

	public TaskConfig getLastTasksConfig() {
		int numChained = this.chainedTasks.size();
		return (numChained == 0) ? config : chainedTasks.get(numChained - 1).getTaskConfig();
	}

	protected S initStub(Class<? super S> stubSuperClass) throws Exception {
		try {
			S stub = config.<S>getStubWrapper(this.userCodeClassLoader).getUserCodeObject(stubSuperClass, this.userCodeClassLoader);
			// check if the class is a subclass, if the check is required
			if (stubSuperClass != null && !stubSuperClass.isAssignableFrom(stub.getClass())) {
				Thread.dumpStack();
				throw new RuntimeException("The class '" + stub.getClass().getName() + "' is not a subclass of '" + 
						stubSuperClass.getName() + "' as is required.");
			}
			stub.setRuntimeContext(this.runtimeUdfContext);
			return stub;
		}
		catch (ClassCastException ccex) {
			throw new Exception("The stub class is not a proper subclass of " + stubSuperClass.getName(), ccex);
		}
	}

	/**
	 * Creates the record readers for the number of inputs as defined by {@link #getNumTaskInputs()}.
	 *
	 * This method requires that the task configuration, the driver, and the user-code class loader are set.
	 */
	@SuppressWarnings("unchecked")
	protected void initInputReaders() throws Exception {
		final int numInputs = getNumTaskInputs();
		final MutableReader<?>[] inputReaders = new MutableReader[numInputs];

		int numGates = 0;

		for (int i = 0; i < numInputs; i++) {
			//  ---------------- create the input readers ---------------------
			// in case where a logical input unions multiple physical inputs, create a union reader
			final int groupSize = this.config.getGroupSize(i);
			numGates += groupSize;
			if (groupSize == 1) {
				// non-union case
				inputReaders[i] = new MutableRecordReader<IOReadableWritable>(this);
			} else if (groupSize > 1){
				// union case
				MutableRecordReader<IOReadableWritable>[] readers = new MutableRecordReader[groupSize];
				for (int j = 0; j < groupSize; ++j) {
					readers[j] = new MutableRecordReader<IOReadableWritable>(this);
				}
				inputReaders[i] = new MutableUnionRecordReader<IOReadableWritable>(readers);
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
	 * Creates the record readers for the extra broadcast inputs as configured by {@link TaskConfig#getNumBroadcastInputs()}.
	 *
	 * This method requires that the task configuration, the driver, and the user-code class loader are set.
	 */
	@SuppressWarnings("unchecked")
	protected void initBroadcastInputReaders() throws Exception {
		final int numBroadcastInputs = this.config.getNumBroadcastInputs();
		final MutableReader<?>[] broadcastInputReaders = new MutableReader[numBroadcastInputs];
		
		for (int i = 0; i < this.config.getNumBroadcastInputs(); i++) {
			//  ---------------- create the input readers ---------------------
			// in case where a logical input unions multiple physical inputs, create a union reader
			final int groupSize = this.config.getBroadcastGroupSize(i);
			if (groupSize == 1) {
				// non-union case
				broadcastInputReaders[i] = new MutableRecordReader<IOReadableWritable>(this);
			} else if (groupSize > 1){
				// union case
				MutableRecordReader<IOReadableWritable>[] readers = new MutableRecordReader[groupSize];
				for (int j = 0; j < groupSize; ++j) {
					readers[j] = new MutableRecordReader<IOReadableWritable>(this);
				}
				broadcastInputReaders[i] = new MutableUnionRecordReader<IOReadableWritable>(readers);
			} else {
				throw new Exception("Illegal input group size in task configuration: " + groupSize);
			}
		}
		this.broadcastInputReaders = broadcastInputReaders;
	}
	
	/**
	 * Creates all the serializers and comparators.
	 */
	protected void initInputsSerializersAndComparators(int numInputs) throws Exception {
		this.inputSerializers = new TypeSerializerFactory<?>[numInputs];
		this.inputComparators = this.driver.requiresComparatorOnInput() ? new TypeComparator[numInputs] : null;
		this.inputIterators = new MutableObjectIterator[numInputs];
		
		for (int i = 0; i < numInputs; i++) {
			//  ---------------- create the serializer first ---------------------
			final TypeSerializerFactory<?> serializerFactory = this.config.getInputSerializer(i, this.userCodeClassLoader);
			this.inputSerializers[i] = serializerFactory;
			
			//  ---------------- create the driver's comparator ---------------------
			if (this.inputComparators != null) {
				final TypeComparatorFactory<?> comparatorFactory = this.config.getDriverComparator(i, this.userCodeClassLoader);
				this.inputComparators[i] = comparatorFactory.createComparator();
			}

			this.inputIterators[i] = createInputIterator(this.inputReaders[i], this.inputSerializers[i]);
		}
	}
	
	/**
	 * Creates all the serializers and iterators for the broadcast inputs.
	 */
	protected void initBroadcastInputsSerializers(int numBroadcastInputs) throws Exception {
		this.broadcastInputSerializers = new TypeSerializerFactory[numBroadcastInputs];
		this.broadcastInputIterators = new MutableObjectIterator[numBroadcastInputs];

		for (int i = 0; i < numBroadcastInputs; i++) {
			//  ---------------- create the serializer first ---------------------
			final TypeSerializerFactory<?> serializerFactory = this.config.getBroadcastInputSerializer(i, this.userCodeClassLoader);
			this.broadcastInputSerializers[i] = serializerFactory;

			this.broadcastInputIterators[i] = createInputIterator(this.broadcastInputReaders[i], this.broadcastInputSerializers[i]);
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
		this.materializationMemory = new int[numInputs];

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
			final int memoryPages;
			final boolean async = this.config.isInputAsynchronouslyMaterialized(i);
			final boolean cached =  this.config.isInputCached(i);

			this.inputIsAsyncMaterialized[i] = async;
			this.inputIsCached[i] = cached;

			if (async || cached) {
				memoryPages = memMan.computeNumberOfPages(this.config.getRelativeInputMaterializationMemory(i));
				if (memoryPages <= 0) {
					throw new Exception("Input marked as materialized/cached, but no memory for materialization provided.");
				}
				this.materializationMemory[i] = memoryPages;
			} else {
				memoryPages = 0;
			}

			if (async) {
				@SuppressWarnings({ "unchecked", "rawtypes" })
				TempBarrier<?> barrier = new TempBarrier(this, getInput(i), this.inputSerializers[i], memMan, ioMan, memoryPages);
				barrier.startReading();
				this.tempBarriers[i] = barrier;
				this.inputs[i] = null;
			} else if (cached) {
				@SuppressWarnings({ "unchecked", "rawtypes" })
				SpillingResettableMutableObjectIterator<?> iter = new SpillingResettableMutableObjectIterator(
					getInput(i), this.inputSerializers[i].getSerializer(), getMemoryManager(), getIOManager(), memoryPages, this);
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
						this.resettableInputs[i].consumeAndCacheRemainingData();
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
						final int pages = this.materializationMemory[i];
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
					this.config.getRelativeMemoryInput(inputNum), this.config.getFilehandlesInput(inputNum),
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
					throw new IllegalStateException("Performing combining sort outside a (group)reduce task!");
				}

				// instantiate ourselves a combiner. we should not use the stub, because the sort and the
				// subsequent (group)reduce would otherwise share it multi-threaded
				final Class<S> userCodeFunctionType = this.driver.getStubType();
				if (userCodeFunctionType == null) {
					throw new IllegalStateException("Performing combining sort outside a reduce task!");
				}
				final S localStub;
				try {
					localStub = initStub(userCodeFunctionType);
				} catch (Exception e) {
					throw new RuntimeException("Initializing the user code and the configuration failed" +
						e.getMessage() == null ? "." : ": " + e.getMessage(), e);
				}
				
				if (!(localStub instanceof GenericCombine)) {
					throw new IllegalStateException("Performing combining sort outside a reduce task!");
				}

				@SuppressWarnings({ "rawtypes", "unchecked" })
				CombiningUnilateralSortMerger<?> cSorter = new CombiningUnilateralSortMerger(
					(GenericCombine) localStub, getMemoryManager(), getIOManager(), this.inputIterators[inputNum], 
					this, this.inputSerializers[inputNum], getLocalStrategyComparator(inputNum),
					this.config.getRelativeMemoryInput(inputNum), this.config.getFilehandlesInput(inputNum),
					this.config.getSpillingThresholdInput(inputNum));
				cSorter.setUdfConfiguration(this.config.getStubParameters());

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
	
	protected MutableObjectIterator<?> createInputIterator(MutableReader<?> inputReader, TypeSerializerFactory<?> serializerFactory) {

		if (serializerFactory.getDataType().equals(Record.class)) {
			// record specific deserialization
			@SuppressWarnings("unchecked")
			MutableReader<Record> reader = (MutableReader<Record>) inputReader;
			return new RecordReaderIterator(reader);
		} else {
			// generic data type serialization
			@SuppressWarnings("unchecked")
			MutableReader<DeserializationDelegate<?>> reader = (MutableReader<DeserializationDelegate<?>>) inputReader;
			@SuppressWarnings({ "unchecked", "rawtypes" })
			final MutableObjectIterator<?> iter = new ReaderIterator(reader, serializerFactory.getSerializer());
			return iter;
		}
	}

	protected int getNumTaskInputs() {
		return this.driver.getNumberOfInputs();
	}

	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategies for each writer.
	 */
	protected void initOutputs() throws Exception {
		this.chainedTasks = new ArrayList<ChainedDriver<?, ?>>();
		this.eventualOutputs = new ArrayList<BufferWriter>();
		this.output = initOutputs(this, this.userCodeClassLoader, this.config, this.chainedTasks, this.eventualOutputs);
	}

	public RuntimeUDFContext createRuntimeContext(String taskName) {
		Environment env = getEnvironment();
		return new RuntimeUDFContext(taskName, env.getCurrentNumberOfSubtasks(), env.getIndexInSubtaskGroup(), env.getCopyTask());
	}

	// --------------------------------------------------------------------------------------------
	//                                   Task Context Signature
	// -------------------------------------------------------------------------------------------

	@Override
	public TaskConfig getTaskConfig() {
		return this.config;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return getEnvironment().getMemoryManager();
	}

	@Override
	public IOManager getIOManager() {
		return getEnvironment().getIOManager();
	}

	@Override
	public S getStub() {
		return this.stub;
	}

	@Override
	public Collector<OT> getOutputCollector() {
		return this.output;
	}

	@Override
	public AbstractInvokable getOwningNepheleTask() {
		return this;
	}

	@Override
	public String formatLogString(String message) {
		return constructLogString(message, getEnvironment().getTaskName(), this);
	}

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


	@Override
	public <X> TypeSerializerFactory<X> getInputSerializer(int index) {
		if (index < 0 || index >= this.driver.getNumberOfInputs()) {
			throw new IndexOutOfBoundsException();
		}

		@SuppressWarnings("unchecked")
		final TypeSerializerFactory<X> serializerFactory = (TypeSerializerFactory<X>) this.inputSerializers[index];
		return serializerFactory;
	}


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
	public static String constructLogString(String message, String taskName, AbstractInvokable parent) {
		return message + ":  " + taskName + " (" + (parent.getEnvironment().getIndexInSubtaskGroup() + 1) +
				'/' + parent.getEnvironment().getCurrentNumberOfSubtasks() + ')';
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
	public static void logAndThrowException(Exception ex, AbstractInvokable parent) throws Exception {
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
			LOG.error(constructLogString("Error in task code", taskName, parent), ex);
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
	public static <T> Collector<T> getOutputCollector(AbstractInvokable task, TaskConfig config, ClassLoader cl, List<BufferWriter> eventualOutputs, int numOutputs)
	throws Exception
	{
		if (numOutputs <= 0) {
			throw new Exception("BUG: The task must have at least one output");
		}

		// get the factory for the serializer
		final TypeSerializerFactory<T> serializerFactory = config.getOutputSerializer(cl);

		// special case the Record
		if (serializerFactory.getDataType().equals(Record.class)) {
			final List<RecordWriter<Record>> writers = new ArrayList<RecordWriter<Record>>(numOutputs);

			// create a writer for each output
			for (int i = 0; i < numOutputs; i++) {
				// create the OutputEmitter from output ship strategy
				final ShipStrategyType strategy = config.getOutputShipStrategy(i);
				final TypeComparatorFactory<?> compFact = config.getOutputComparator(i, cl);
				final RecordOutputEmitter oe;
				if (compFact == null) {
					oe = new RecordOutputEmitter(strategy);
				} else {
					@SuppressWarnings("unchecked")
					TypeComparator<Record> comparator = (TypeComparator<Record>) compFact.createComparator();
					if (!comparator.supportsCompareAgainstReference()) {
						throw new Exception("Incompatibe serializer-/comparator factories.");
					}
					final DataDistribution distribution = config.getOutputDataDistribution(i, cl);
					oe = new RecordOutputEmitter(strategy, comparator, distribution);
				}

				writers.add(new RecordWriter<Record>(task, oe));
			}
			if (eventualOutputs != null) {
				eventualOutputs.addAll(writers);
			}

			@SuppressWarnings("unchecked")
			final Collector<T> outColl = (Collector<T>) new RecordOutputCollector(writers);
			return outColl;
		}
		else {
			// generic case
			final List<RecordWriter<SerializationDelegate<T>>> writers = new ArrayList<RecordWriter<SerializationDelegate<T>>>(numOutputs);

			// create a writer for each output
			for (int i = 0; i < numOutputs; i++)
			{
				// create the OutputEmitter from output ship strategy
				final ShipStrategyType strategy = config.getOutputShipStrategy(i);
				final TypeComparatorFactory<T> compFactory = config.getOutputComparator(i, cl);
				final DataDistribution dataDist = config.getOutputDataDistribution(i, cl);

				final ChannelSelector<SerializationDelegate<T>> oe;
				if (compFactory == null) {
					oe = new OutputEmitter<T>(strategy);
				} else if (dataDist == null){
					final TypeComparator<T> comparator = compFactory.createComparator();
					oe = new OutputEmitter<T>(strategy, comparator);
				} else {
					final TypeComparator<T> comparator = compFactory.createComparator();
					oe = new OutputEmitter<T>(strategy, comparator, dataDist);
				}

				writers.add(new RecordWriter<SerializationDelegate<T>>(task, oe));
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
					List<ChainedDriver<?, ?>> chainedTasksTarget, List<BufferWriter> eventualOutputs)
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
					Class<? extends ChainedDriver<?, ?>> ctc = config.getChainedTask(i);
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

				ct.setup(chainedStubConf, taskName, previous, nepheleTask, cl);
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

	public static void initOutputWriters(List<BufferWriter> writers) {
		for (BufferWriter writer : writers) {
			((RecordWriter<?>) writer).initializeSerializers();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  User Code LifeCycle
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Opens the given stub using its {@link Function#open(Configuration)} method. If the open call produces
	 * an exception, a new exception with a standard error message is created, using the encountered exception
	 * as its cause.
	 * 
	 * @param stub The user code instance to be opened.
	 * @param parameters The parameters supplied to the user code.
	 * 
	 * @throws Exception Thrown, if the user code's open method produces an exception.
	 */
	public static void openUserCode(Function stub, Configuration parameters) throws Exception {
		try {
			stub.open(parameters);
		} catch (Throwable t) {
			throw new Exception("The user defined 'open(Configuration)' method in " + stub.getClass().toString() + " caused an exception: " + t.getMessage(), t);
		}
	}
	
	/**
	 * Closes the given stub using its {@link Function#close()} method. If the close call produces
	 * an exception, a new exception with a standard error message is created, using the encountered exception
	 * as its cause.
	 * 
	 * @param stub The user code instance to be closed.
	 * 
	 * @throws Exception Thrown, if the user code's close method produces an exception.
	 */
	public static void closeUserCode(Function stub) throws Exception {
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
			if (LOG.isDebugEnabled()) {
				LOG.debug(constructLogString("Start task code", task.getTaskName(), parent));
			}
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
			
			if (LOG.isDebugEnabled()) {
				LOG.debug(constructLogString("Finished task code", task.getTaskName(), parent));
			}
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
		try {
			T stub = config.<T>getStubWrapper(cl).getUserCodeObject(superClass, cl);
			// check if the class is a subclass, if the check is required
			if (superClass != null && !superClass.isAssignableFrom(stub.getClass())) {
				throw new RuntimeException("The class '" + stub.getClass().getName() + "' is not a subclass of '" + 
						superClass.getName() + "' as is required.");
			}
			return stub;
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The UDF class is not a proper subclass of " + superClass.getName(), ccex);
		}
	}
	
	private static int[] asArray(List<Integer> list) {
		int[] a = new int[list.size()];
		
		int i = 0;
		for (int val : list) {
			a[i++] = val;
		}
		return a;
	}
}
