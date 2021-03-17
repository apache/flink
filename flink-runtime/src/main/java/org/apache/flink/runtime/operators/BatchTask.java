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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.broadcast.BroadcastVariableMaterialization;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.runtime.operators.resettable.SpillingResettableMutableObjectIterator;
import org.apache.flink.runtime.operators.shipping.OutputCollector;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.operators.util.CloseableInputProvider;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * The base class for all batch tasks. Encapsulated common behavior and implements the main
 * life-cycle of the user code.
 */
public class BatchTask<S extends Function, OT> extends AbstractInvokable
        implements TaskContext<S, OT> {

    protected static final Logger LOG = LoggerFactory.getLogger(BatchTask.class);

    // --------------------------------------------------------------------------------------------

    /**
     * The driver that invokes the user code (the stub implementation). The central driver in this
     * task (further drivers may be chained behind this driver).
     */
    protected volatile Driver<S, OT> driver;

    /**
     * The instantiated user code of this task's main operator (driver). May be null if the operator
     * has no udf.
     */
    protected S stub;

    /** The udf's runtime context. */
    protected DistributedRuntimeUDFContext runtimeUdfContext;

    /**
     * The collector that forwards the user code's results. May forward to a channel or to chained
     * drivers within this task.
     */
    protected Collector<OT> output;

    /**
     * The output writers for the data that this task forwards to the next task. The latest driver
     * (the central, if no chained drivers exist, otherwise the last chained driver) produces its
     * output to these writers.
     */
    protected List<RecordWriter<?>> eventualOutputs;

    /** The input readers of this task. */
    protected MutableReader<?>[] inputReaders;

    /** The input readers for the configured broadcast variables for this task. */
    protected MutableReader<?>[] broadcastInputReaders;

    /** The inputs reader, wrapped in an iterator. Prior to the local strategies, etc... */
    protected MutableObjectIterator<?>[] inputIterators;

    /** The indices of the iterative inputs. Empty, if the task is not iterative. */
    protected int[] iterativeInputs;

    /** The indices of the iterative broadcast inputs. Empty, if non of the inputs is iterative. */
    protected int[] iterativeBroadcastInputs;

    /** The local strategies that are applied on the inputs. */
    protected volatile CloseableInputProvider<?>[] localStrategies;

    /**
     * The optional temp barriers on the inputs for dead-lock breaking. Are optionally resettable.
     */
    protected volatile TempBarrier<?>[] tempBarriers;

    /** The resettable inputs in the case where no temp barrier is needed. */
    protected volatile SpillingResettableMutableObjectIterator<?>[] resettableInputs;

    /**
     * The inputs to the operator. Return the readers' data after the application of the local
     * strategy and the temp-table barrier.
     */
    protected MutableObjectIterator<?>[] inputs;

    /** The serializers for the input data type. */
    protected TypeSerializerFactory<?>[] inputSerializers;

    /** The serializers for the broadcast input data types. */
    protected TypeSerializerFactory<?>[] broadcastInputSerializers;

    /** The comparators for the central driver. */
    protected TypeComparator<?>[] inputComparators;

    /** The task configuration with the setup parameters. */
    protected TaskConfig config;

    /** A list of chained drivers, if there are any. */
    protected ArrayList<ChainedDriver<?, ?>> chainedTasks;

    /**
     * Certain inputs may be excluded from resetting. For example, the initial partial solution in
     * an iteration head must not be reset (it is read through the back channel), when all others
     * are reset.
     */
    private boolean[] excludeFromReset;

    /** Flag indicating for each input whether it is cached and can be reset. */
    private boolean[] inputIsCached;

    /** flag indicating for each input whether it must be asynchronously materialized. */
    private boolean[] inputIsAsyncMaterialized;

    /** The amount of memory per input that is dedicated to the materialization. */
    private int[] materializationMemory;

    /** The flag that tags the task as still running. Checked periodically to abort processing. */
    protected volatile boolean running = true;

    /** The accumulator map used in the RuntimeContext. */
    protected Map<String, Accumulator<?, ?>> accumulatorMap;

    private OperatorMetricGroup metrics;

    // --------------------------------------------------------------------------------------------
    //                                  Constructor
    // --------------------------------------------------------------------------------------------

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public BatchTask(Environment environment) {
        super(environment);
    }

    // --------------------------------------------------------------------------------------------
    //                                  Task Interface
    // --------------------------------------------------------------------------------------------

    /** The main work method. */
    @Override
    public void invoke() throws Exception {
        // --------------------------------------------------------------------
        // Initialize
        // --------------------------------------------------------------------
        if (LOG.isDebugEnabled()) {
            LOG.debug(formatLogString("Start registering input and output."));
        }

        // obtain task configuration (including stub parameters)
        Configuration taskConf = getTaskConfiguration();
        this.config = new TaskConfig(taskConf);

        // now get the operator class which drives the operation
        final Class<? extends Driver<S, OT>> driverClass = this.config.getDriver();
        this.driver = InstantiationUtil.instantiate(driverClass, Driver.class);

        String headName = getEnvironment().getTaskInfo().getTaskName().split("->")[0].trim();
        this.metrics =
                getEnvironment()
                        .getMetricGroup()
                        .getOrAddOperator(
                                headName.startsWith("CHAIN") ? headName.substring(6) : headName);
        this.metrics.getIOMetricGroup().reuseInputMetricsForTask();
        if (config.getNumberOfChainedStubs() == 0) {
            this.metrics.getIOMetricGroup().reuseOutputMetricsForTask();
        }

        // initialize the readers.
        // this does not yet trigger any stream consuming or processing.
        initInputReaders();
        initBroadcastInputReaders();

        // initialize the writers.
        initOutputs();

        if (LOG.isDebugEnabled()) {
            LOG.debug(formatLogString("Finished registering input and output."));
        }

        // --------------------------------------------------------------------
        // Invoke
        // --------------------------------------------------------------------
        if (LOG.isDebugEnabled()) {
            LOG.debug(formatLogString("Start task code."));
        }

        this.runtimeUdfContext = createRuntimeContext(metrics);

        // whatever happens in this scope, make sure that the local strategies are cleaned up!
        // note that the initialization of the local strategies is in the try-finally block as well,
        // so that the thread that creates them catches its own errors that may happen in that
        // process.
        // this is especially important, since there may be asynchronous closes (such as through
        // canceling).
        try {
            // initialize the remaining data structures on the input and trigger the local
            // processing
            // the local processing includes building the dams / caches
            try {
                int numInputs = driver.getNumberOfInputs();
                int numComparators = driver.getNumberOfDriverComparators();
                int numBroadcastInputs = this.config.getNumBroadcastInputs();

                initInputsSerializersAndComparators(numInputs, numComparators);
                initBroadcastInputsSerializers(numBroadcastInputs);

                // set the iterative status for inputs and broadcast inputs
                {
                    List<Integer> iterativeInputs = new ArrayList<>();

                    for (int i = 0; i < numInputs; i++) {
                        final int numberOfEventsUntilInterrupt =
                                getTaskConfig().getNumberOfEventsUntilInterruptInIterativeGate(i);

                        if (numberOfEventsUntilInterrupt < 0) {
                            throw new IllegalArgumentException();
                        } else if (numberOfEventsUntilInterrupt > 0) {
                            this.inputReaders[i].setIterativeReader();
                            iterativeInputs.add(i);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                        formatLogString(
                                                "Input ["
                                                        + i
                                                        + "] reads in supersteps with ["
                                                        + numberOfEventsUntilInterrupt
                                                        + "] event(s) till next superstep."));
                            }
                        }
                    }
                    this.iterativeInputs = asArray(iterativeInputs);
                }

                {
                    List<Integer> iterativeBcInputs = new ArrayList<>();

                    for (int i = 0; i < numBroadcastInputs; i++) {
                        final int numberOfEventsUntilInterrupt =
                                getTaskConfig()
                                        .getNumberOfEventsUntilInterruptInIterativeBroadcastGate(i);

                        if (numberOfEventsUntilInterrupt < 0) {
                            throw new IllegalArgumentException();
                        } else if (numberOfEventsUntilInterrupt > 0) {
                            this.broadcastInputReaders[i].setIterativeReader();
                            iterativeBcInputs.add(i);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                        formatLogString(
                                                "Broadcast input ["
                                                        + i
                                                        + "] reads in supersteps with ["
                                                        + numberOfEventsUntilInterrupt
                                                        + "] event(s) till next superstep."));
                            }
                        }
                    }
                    this.iterativeBroadcastInputs = asArray(iterativeBcInputs);
                }

                initLocalStrategies(numInputs);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Initializing the input processing failed"
                                + (e.getMessage() == null ? "." : ": " + e.getMessage()),
                        e);
            }

            if (!this.running) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(formatLogString("Task cancelled before task code was started."));
                }
                return;
            }

            // pre main-function initialization
            initialize();

            // read the broadcast variables. they will be released in the finally clause
            for (int i = 0; i < this.config.getNumBroadcastInputs(); i++) {
                final String name = this.config.getBroadcastInputName(i);
                readAndSetBroadcastInput(
                        i, name, this.runtimeUdfContext, 1 /* superstep one for the start */);
            }

            // the work goes here
            run();
        } finally {
            // clean up in any case!
            closeLocalStrategiesAndCaches();

            clearReaders(inputReaders);
            clearWriters(eventualOutputs);
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

    // --------------------------------------------------------------------------------------------
    //                                  Main Work Methods
    // --------------------------------------------------------------------------------------------

    protected void initialize() throws Exception {
        // create the operator
        try {
            this.driver.setup(this);
        } catch (Throwable t) {
            throw new Exception(
                    "The driver setup for '"
                            + this.getEnvironment().getTaskInfo().getTaskName()
                            + "' , caused an error: "
                            + t.getMessage(),
                    t);
        }

        // instantiate the UDF
        try {
            final Class<? super S> userCodeFunctionType = this.driver.getStubType();
            // if the class is null, the driver has no user code
            if (userCodeFunctionType != null) {
                this.stub = initStub(userCodeFunctionType);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Initializing the UDF" + (e.getMessage() == null ? "." : ": " + e.getMessage()),
                    e);
        }
    }

    protected <X> void readAndSetBroadcastInput(
            int inputNum, String bcVarName, DistributedRuntimeUDFContext context, int superstep)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    formatLogString(
                            "Setting broadcast variable '"
                                    + bcVarName
                                    + "'"
                                    + (superstep > 1 ? ", superstep " + superstep : "")));
        }

        @SuppressWarnings("unchecked")
        final TypeSerializerFactory<X> serializerFactory =
                (TypeSerializerFactory<X>) this.broadcastInputSerializers[inputNum];

        final MutableReader<?> reader = this.broadcastInputReaders[inputNum];

        BroadcastVariableMaterialization<X, ?> variable =
                getEnvironment()
                        .getBroadcastVariableManager()
                        .materializeBroadcastVariable(
                                bcVarName, superstep, this, reader, serializerFactory);
        context.setBroadcastVariable(bcVarName, variable);
    }

    protected void releaseBroadcastVariables(
            String bcVarName, int superstep, DistributedRuntimeUDFContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    formatLogString(
                            "Releasing broadcast variable '"
                                    + bcVarName
                                    + "'"
                                    + (superstep > 1 ? ", superstep " + superstep : "")));
        }

        getEnvironment().getBroadcastVariableManager().releaseReference(bcVarName, superstep, this);
        context.clearBroadcastVariable(bcVarName);
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
            } catch (Throwable t) {
                // if the preparation caused an error, clean up
                // errors during clean-up are swallowed, because we have already a root exception
                throw new Exception(
                        "The data preparation for task '"
                                + this.getEnvironment().getTaskInfo().getTaskName()
                                + "' , caused an error: "
                                + t.getMessage(),
                        t);
            }

            // check for canceling
            if (!this.running) {
                return;
            }

            // start all chained tasks
            BatchTask.openChainedTasks(this.chainedTasks, this);

            // open stub implementation
            if (this.stub != null) {
                try {
                    Configuration stubConfig = this.config.getStubParameters();
                    FunctionUtils.openFunction(this.stub, stubConfig);
                    stubOpen = true;
                } catch (Throwable t) {
                    throw new Exception(
                            "The user defined 'open()' method caused an exception: "
                                    + t.getMessage(),
                            t);
                }
            }

            // run the user code
            this.driver.run();

            // close. We close here such that a regular close throwing an exception marks a task as
            // failed.
            if (this.running && this.stub != null) {
                FunctionUtils.closeFunction(this.stub);
                stubOpen = false;
            }

            // close all chained tasks letting them report failure
            BatchTask.closeChainedTasks(this.chainedTasks, this);

            // close the output collector
            this.output.close();
        } catch (Exception ex) {
            // close the input, but do not report any exceptions, since we already have another root
            // cause
            if (stubOpen) {
                try {
                    FunctionUtils.closeFunction(this.stub);
                } catch (Throwable t) {
                    // do nothing
                }
            }

            // if resettable driver invoke teardown
            if (this.driver instanceof ResettableDriver) {
                final ResettableDriver<?, ?> resDriver = (ResettableDriver<?, ?>) this.driver;
                try {
                    resDriver.teardown();
                } catch (Throwable t) {
                    throw new Exception(
                            "Error while shutting down an iterative operator: " + t.getMessage(),
                            t);
                }
            }

            BatchTask.cancelChainedTasks(this.chainedTasks);

            ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

            if (ex instanceof CancelTaskException) {
                // forward canceling exception
                throw ex;
            } else if (this.running) {
                // throw only if task was not cancelled. in the case of canceling, exceptions are
                // expected
                BatchTask.logAndThrowException(ex, this);
            }
        } finally {
            this.driver.cleanup();
        }
    }

    protected void closeLocalStrategiesAndCaches() {

        // make sure that all broadcast variable references held by this task are released
        if (LOG.isDebugEnabled()) {
            LOG.debug(formatLogString("Releasing all broadcast variables."));
        }

        getEnvironment().getBroadcastVariableManager().releaseAllReferencesFromTask(this);
        if (runtimeUdfContext != null) {
            runtimeUdfContext.clearAllBroadcastVariables();
        }

        // clean all local strategies and caches/pipeline breakers.

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

    /** @return the last output collector in the collector chain */
    @SuppressWarnings("unchecked")
    protected Collector<OT> getLastOutputCollector() {
        int numChained = this.chainedTasks.size();
        return (numChained == 0)
                ? output
                : (Collector<OT>) chainedTasks.get(numChained - 1).getOutputCollector();
    }

    /**
     * Sets the last output {@link Collector} of the collector chain of this {@link BatchTask}.
     *
     * <p>In case of chained tasks, the output collector of the last {@link ChainedDriver} is set.
     * Otherwise it is the single collector of the {@link BatchTask}.
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
            ClassLoader userCodeClassLoader = getUserCodeClassLoader();
            S stub =
                    config.<S>getStubWrapper(userCodeClassLoader)
                            .getUserCodeObject(stubSuperClass, userCodeClassLoader);
            // check if the class is a subclass, if the check is required
            if (stubSuperClass != null && !stubSuperClass.isAssignableFrom(stub.getClass())) {
                throw new RuntimeException(
                        "The class '"
                                + stub.getClass().getName()
                                + "' is not a subclass of '"
                                + stubSuperClass.getName()
                                + "' as is required.");
            }
            FunctionUtils.setFunctionRuntimeContext(stub, this.runtimeUdfContext);
            return stub;
        } catch (ClassCastException ccex) {
            throw new Exception(
                    "The stub class is not a proper subclass of " + stubSuperClass.getName(), ccex);
        }
    }

    /**
     * Creates the record readers for the number of inputs as defined by {@link
     * #getNumTaskInputs()}. This method requires that the task configuration, the driver, and the
     * user-code class loader are set.
     */
    protected void initInputReaders() throws Exception {
        final int numInputs = getNumTaskInputs();
        final MutableReader<?>[] inputReaders = new MutableReader<?>[numInputs];

        int currentReaderOffset = 0;

        for (int i = 0; i < numInputs; i++) {
            //  ---------------- create the input readers ---------------------
            // in case where a logical input unions multiple physical inputs, create a union reader
            final int groupSize = this.config.getGroupSize(i);

            if (groupSize == 1) {
                // non-union case
                inputReaders[i] =
                        new MutableRecordReader<>(
                                getEnvironment().getInputGate(currentReaderOffset),
                                getEnvironment().getTaskManagerInfo().getTmpDirectories());
            } else if (groupSize > 1) {
                // union case
                IndexedInputGate[] readers = new IndexedInputGate[groupSize];
                for (int j = 0; j < groupSize; ++j) {
                    readers[j] = getEnvironment().getInputGate(currentReaderOffset + j);
                }
                inputReaders[i] =
                        new MutableRecordReader<>(
                                new UnionInputGate(readers),
                                getEnvironment().getTaskManagerInfo().getTmpDirectories());
            } else {
                throw new Exception("Illegal input group size in task configuration: " + groupSize);
            }

            currentReaderOffset += groupSize;
        }
        this.inputReaders = inputReaders;

        // final sanity check
        if (currentReaderOffset != this.config.getNumInputs()) {
            throw new Exception(
                    "Illegal configuration: Number of input gates and group sizes are not consistent.");
        }
    }

    /**
     * Creates the record readers for the extra broadcast inputs as configured by {@link
     * TaskConfig#getNumBroadcastInputs()}. This method requires that the task configuration, the
     * driver, and the user-code class loader are set.
     */
    protected void initBroadcastInputReaders() throws Exception {
        final int numBroadcastInputs = this.config.getNumBroadcastInputs();
        final MutableReader<?>[] broadcastInputReaders = new MutableReader<?>[numBroadcastInputs];

        int currentReaderOffset = config.getNumInputs();

        for (int i = 0; i < this.config.getNumBroadcastInputs(); i++) {
            //  ---------------- create the input readers ---------------------
            // in case where a logical input unions multiple physical inputs, create a union reader
            final int groupSize = this.config.getBroadcastGroupSize(i);
            if (groupSize == 1) {
                // non-union case
                broadcastInputReaders[i] =
                        new MutableRecordReader<>(
                                getEnvironment().getInputGate(currentReaderOffset),
                                getEnvironment().getTaskManagerInfo().getTmpDirectories());
            } else if (groupSize > 1) {
                // union case
                IndexedInputGate[] readers = new IndexedInputGate[groupSize];
                for (int j = 0; j < groupSize; ++j) {
                    readers[j] = getEnvironment().getInputGate(currentReaderOffset + j);
                }
                broadcastInputReaders[i] =
                        new MutableRecordReader<>(
                                new UnionInputGate(readers),
                                getEnvironment().getTaskManagerInfo().getTmpDirectories());
            } else {
                throw new Exception("Illegal input group size in task configuration: " + groupSize);
            }

            currentReaderOffset += groupSize;
        }
        this.broadcastInputReaders = broadcastInputReaders;
    }

    /** Creates all the serializers and comparators. */
    protected void initInputsSerializersAndComparators(int numInputs, int numComparators) {
        this.inputSerializers = new TypeSerializerFactory<?>[numInputs];
        this.inputComparators = numComparators > 0 ? new TypeComparator<?>[numComparators] : null;
        this.inputIterators = new MutableObjectIterator<?>[numInputs];

        ClassLoader userCodeClassLoader = getUserCodeClassLoader();

        for (int i = 0; i < numInputs; i++) {

            final TypeSerializerFactory<?> serializerFactory =
                    this.config.getInputSerializer(i, userCodeClassLoader);
            this.inputSerializers[i] = serializerFactory;

            this.inputIterators[i] =
                    createInputIterator(this.inputReaders[i], this.inputSerializers[i]);
        }

        //  ---------------- create the driver's comparators ---------------------
        for (int i = 0; i < numComparators; i++) {

            if (this.inputComparators != null) {
                final TypeComparatorFactory<?> comparatorFactory =
                        this.config.getDriverComparator(i, userCodeClassLoader);
                this.inputComparators[i] = comparatorFactory.createComparator();
            }
        }
    }

    /** Creates all the serializers and iterators for the broadcast inputs. */
    protected void initBroadcastInputsSerializers(int numBroadcastInputs) {
        this.broadcastInputSerializers = new TypeSerializerFactory<?>[numBroadcastInputs];

        ClassLoader userCodeClassLoader = getUserCodeClassLoader();

        for (int i = 0; i < numBroadcastInputs; i++) {
            //  ---------------- create the serializer first ---------------------
            final TypeSerializerFactory<?> serializerFactory =
                    this.config.getBroadcastInputSerializer(i, userCodeClassLoader);
            this.broadcastInputSerializers[i] = serializerFactory;
        }
    }

    /**
     * NOTE: This method must be invoked after the invocation of {@code #initInputReaders()} and
     * {@code #initInputSerializersAndComparators(int)}!
     */
    protected void initLocalStrategies(int numInputs) throws Exception {

        final MemoryManager memMan = getMemoryManager();
        final IOManager ioMan = getIOManager();

        this.localStrategies = new CloseableInputProvider<?>[numInputs];
        this.inputs = new MutableObjectIterator<?>[numInputs];
        this.excludeFromReset = new boolean[numInputs];
        this.inputIsCached = new boolean[numInputs];
        this.inputIsAsyncMaterialized = new boolean[numInputs];
        this.materializationMemory = new int[numInputs];

        // set up the local strategies first, such that the can work before any temp barrier is
        // created
        for (int i = 0; i < numInputs; i++) {
            initInputLocalStrategy(i);
        }

        // we do another loop over the inputs, because we want to instantiate all
        // sorters, etc before requesting the first input (as this call may block)

        // we have two types of materialized inputs, and both are replayable (can act as a cache)
        // The first variant materializes in a different thread and hence
        // acts as a pipeline breaker. this one should only be there, if a pipeline breaker is
        // needed.
        // the second variant spills to the side and will not read unless the result is also
        // consumed
        // in a pipelined fashion.
        this.resettableInputs = new SpillingResettableMutableObjectIterator<?>[numInputs];
        this.tempBarriers = new TempBarrier<?>[numInputs];

        for (int i = 0; i < numInputs; i++) {
            final int memoryPages;
            final boolean async = this.config.isInputAsynchronouslyMaterialized(i);
            final boolean cached = this.config.isInputCached(i);

            this.inputIsAsyncMaterialized[i] = async;
            this.inputIsCached[i] = cached;

            if (async || cached) {
                memoryPages =
                        memMan.computeNumberOfPages(
                                this.config.getRelativeInputMaterializationMemory(i));
                if (memoryPages <= 0) {
                    throw new Exception(
                            "Input marked as materialized/cached, but no memory for materialization provided.");
                }
                this.materializationMemory[i] = memoryPages;
            } else {
                memoryPages = 0;
            }

            if (async) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                TempBarrier<?> barrier =
                        new TempBarrier(
                                this,
                                getInput(i),
                                this.inputSerializers[i],
                                memMan,
                                ioMan,
                                memoryPages,
                                emptyList());
                barrier.startReading();
                this.tempBarriers[i] = barrier;
                this.inputs[i] = null;
            } else if (cached) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                SpillingResettableMutableObjectIterator<?> iter =
                        new SpillingResettableMutableObjectIterator(
                                getInput(i),
                                this.inputSerializers[i].getSerializer(),
                                getMemoryManager(),
                                getIOManager(),
                                memoryPages,
                                this);
                this.resettableInputs[i] = iter;
                this.inputs[i] = iter;
            }
        }
    }

    protected void resetAllInputs() throws Exception {

        // first we need to make sure that caches consume remaining data
        // NOTE: we need to do this before closing the local strategies
        for (int i = 0; i < this.inputs.length; i++) {

            if (this.inputIsCached[i] && this.resettableInputs[i] != null) {
                this.resettableInputs[i].consumeAndCacheRemainingData();
            }
        }

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
                        throw new RuntimeException(
                                "Found a resettable input, but no temp barrier and no resettable iterator.");
                    }
                } else {
                    // close the async barrier if there is one
                    List<MemorySegment> allocated =
                            tempBarriers[i] == null
                                    ? emptyList()
                                    : tempBarriers[i].closeAndGetLeftoverMemory();
                    tempBarriers[i] = null;

                    try {
                        initInputLocalStrategy(i);

                        if (this.inputIsAsyncMaterialized[i]) {
                            final int pages = this.materializationMemory[i];
                            Preconditions.checkState(
                                    allocated.size()
                                            <= pages); // pages shouldn't change, but some segments
                            // might have been consumed
                            @SuppressWarnings({"unchecked", "rawtypes"})
                            TempBarrier<?> barrier =
                                    new TempBarrier(
                                            this,
                                            getInput(i),
                                            this.inputSerializers[i],
                                            memMan,
                                            ioMan,
                                            pages,
                                            allocated);
                            barrier.startReading();
                            this.tempBarriers[i] = barrier;
                            this.inputs[i] = null;
                        } else {
                            memMan.release(allocated);
                        }
                    } catch (Exception exception) {
                        try {
                            memMan.release(allocated);
                        } catch (Exception releaseException) {
                            exception.addSuppressed(releaseException);
                        }
                        throw exception;
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
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    Sorter<?> sorter =
                            ExternalSorter.newBuilder(
                                            getMemoryManager(),
                                            this,
                                            this.inputSerializers[inputNum].getSerializer(),
                                            getLocalStrategyComparator(inputNum))
                                    .maxNumFileHandles(this.config.getFilehandlesInput(inputNum))
                                    .enableSpilling(
                                            getIOManager(),
                                            this.config.getSpillingThresholdInput(inputNum))
                                    .memoryFraction(this.config.getRelativeMemoryInput(inputNum))
                                    .objectReuse(this.getExecutionConfig().isObjectReuseEnabled())
                                    .largeRecords(this.getTaskConfig().getUseLargeRecordHandler())
                                    .build((MutableObjectIterator) this.inputIterators[inputNum]);
                    // set the input to null such that it will be lazily fetched from the input
                    // strategy
                    this.inputs[inputNum] = null;
                    this.localStrategies[inputNum] = sorter;
                    break;
                case COMBININGSORT:
                    // sanity check this special case!
                    // this still breaks a bit of the abstraction!
                    // we should have nested configurations for the local strategies to solve that
                    if (inputNum != 0) {
                        throw new IllegalStateException(
                                "Performing combining sort outside a (group)reduce task!");
                    }

                    // instantiate ourselves a combiner. we should not use the stub, because the
                    // sort and the
                    // subsequent (group)reduce would otherwise share it multi-threaded
                    final Class<S> userCodeFunctionType = this.driver.getStubType();
                    if (userCodeFunctionType == null) {
                        throw new IllegalStateException(
                                "Performing combining sort outside a reduce task!");
                    }
                    final S localStub;
                    try {
                        localStub = initStub(userCodeFunctionType);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Initializing the user code and the configuration failed"
                                        + (e.getMessage() == null ? "." : ": " + e.getMessage()),
                                e);
                    }

                    if (!(localStub instanceof GroupCombineFunction)) {
                        throw new IllegalStateException(
                                "Performing combining sort outside a reduce task!");
                    }

                    @SuppressWarnings({"rawtypes", "unchecked"})
                    Sorter<?> cSorter =
                            ExternalSorter.newBuilder(
                                            getMemoryManager(),
                                            this,
                                            this.inputSerializers[inputNum].getSerializer(),
                                            getLocalStrategyComparator(inputNum))
                                    .maxNumFileHandles(this.config.getFilehandlesInput(inputNum))
                                    .withCombiner(
                                            (GroupCombineFunction) localStub,
                                            this.config.getStubParameters())
                                    .enableSpilling(
                                            getIOManager(),
                                            this.config.getSpillingThresholdInput(inputNum))
                                    .memoryFraction(this.config.getRelativeMemoryInput(inputNum))
                                    .objectReuse(this.getExecutionConfig().isObjectReuseEnabled())
                                    .largeRecords(this.getTaskConfig().getUseLargeRecordHandler())
                                    .build(this.inputIterators[inputNum]);

                    // set the input to null such that it will be lazily fetched from the input
                    // strategy
                    this.inputs[inputNum] = null;
                    this.localStrategies[inputNum] = cSorter;
                    break;
                default:
                    throw new Exception(
                            "Unrecognized local strategy provided: " + localStrategy.name());
            }
        } else {
            // no local strategy in the config
            this.inputs[inputNum] = this.inputIterators[inputNum];
        }
    }

    private <T> TypeComparator<T> getLocalStrategyComparator(int inputNum) throws Exception {
        TypeComparatorFactory<T> compFact =
                this.config.getInputComparator(inputNum, getUserCodeClassLoader());
        if (compFact == null) {
            throw new Exception(
                    "Missing comparator factory for local strategy on input " + inputNum);
        }
        return compFact.createComparator();
    }

    protected MutableObjectIterator<?> createInputIterator(
            MutableReader<?> inputReader, TypeSerializerFactory<?> serializerFactory) {
        @SuppressWarnings("unchecked")
        MutableReader<DeserializationDelegate<?>> reader =
                (MutableReader<DeserializationDelegate<?>>) inputReader;
        @SuppressWarnings({"unchecked", "rawtypes"})
        final MutableObjectIterator<?> iter =
                new ReaderIterator(reader, serializerFactory.getSerializer());
        return iter;
    }

    protected int getNumTaskInputs() {
        return this.driver.getNumberOfInputs();
    }

    /**
     * Creates a writer for each output. Creates an OutputCollector which forwards its input to all
     * writers. The output collector applies the configured shipping strategies for each writer.
     */
    protected void initOutputs() throws Exception {
        this.chainedTasks = new ArrayList<>();
        this.eventualOutputs = new ArrayList<>();

        this.accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

        this.output =
                initOutputs(
                        this,
                        getEnvironment().getUserCodeClassLoader(),
                        this.config,
                        this.chainedTasks,
                        this.eventualOutputs,
                        this.getExecutionConfig(),
                        this.accumulatorMap);
    }

    public DistributedRuntimeUDFContext createRuntimeContext(MetricGroup metrics) {
        Environment env = getEnvironment();

        return new DistributedRuntimeUDFContext(
                env.getTaskInfo(),
                env.getUserCodeClassLoader(),
                getExecutionConfig(),
                env.getDistributedCacheEntries(),
                this.accumulatorMap,
                metrics,
                env.getExternalResourceInfoProvider(),
                env.getJobID());
    }

    // --------------------------------------------------------------------------------------------
    //                                   Task Context Signature
    // -------------------------------------------------------------------------------------------

    @Override
    public TaskConfig getTaskConfig() {
        return this.config;
    }

    @Override
    public TaskManagerRuntimeInfo getTaskManagerInfo() {
        return getEnvironment().getTaskManagerInfo();
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
    public AbstractInvokable getContainingTask() {
        return this;
    }

    @Override
    public String formatLogString(String message) {
        return constructLogString(message, getEnvironment().getTaskInfo().getTaskName(), this);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return metrics;
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
                    MutableObjectIterator<X> iter =
                            (MutableObjectIterator<X>) this.tempBarriers[index].getIterator();
                    in = iter;
                } else if (this.localStrategies[index] != null) {
                    @SuppressWarnings("unchecked")
                    MutableObjectIterator<X> iter =
                            (MutableObjectIterator<X>) this.localStrategies[index].getIterator();
                    in = iter;
                } else {
                    throw new RuntimeException(
                            "Bug: null input iterator, null temp barrier, and null local strategy.");
                }
                this.inputs[index] = in;
                return in;
            } catch (InterruptedException iex) {
                throw new RuntimeException(
                        "Interrupted while waiting for input " + index + " to become available.");
            } catch (IOException ioex) {
                throw new RuntimeException(
                        "An I/O Exception occurred while obtaining input " + index + ".");
            }
        }
    }

    @Override
    public <X> TypeSerializerFactory<X> getInputSerializer(int index) {
        if (index < 0 || index >= this.driver.getNumberOfInputs()) {
            throw new IndexOutOfBoundsException();
        }

        @SuppressWarnings("unchecked")
        final TypeSerializerFactory<X> serializerFactory =
                (TypeSerializerFactory<X>) this.inputSerializers[index];
        return serializerFactory;
    }

    @Override
    public <X> TypeComparator<X> getDriverComparator(int index) {
        if (this.inputComparators == null) {
            throw new IllegalStateException("Comparators have not been created!");
        } else if (index < 0 || index >= this.driver.getNumberOfDriverComparators()) {
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
     * Utility function that composes a string for logging purposes. The string includes the given
     * message, the given name of the task and the index in its subtask group as well as the number
     * of instances that exist in its subtask group.
     *
     * @param message The main message for the log.
     * @param taskName The name of the task.
     * @param parent The task that contains the code producing the message.
     * @return The string for logging.
     */
    public static String constructLogString(
            String message, String taskName, AbstractInvokable parent) {
        return message
                + ":  "
                + taskName
                + " ("
                + (parent.getEnvironment().getTaskInfo().getIndexOfThisSubtask() + 1)
                + '/'
                + parent.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks()
                + ')';
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
    public static void logAndThrowException(Exception ex, AbstractInvokable parent)
            throws Exception {
        String taskName;
        if (ex instanceof ExceptionInChainedStubException) {
            do {
                ExceptionInChainedStubException cex = (ExceptionInChainedStubException) ex;
                taskName = cex.getTaskName();
                ex = cex.getWrappedException();
            } while (ex instanceof ExceptionInChainedStubException);
        } else {
            taskName = parent.getEnvironment().getTaskInfo().getTaskName();
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
     * Creates the {@link Collector} for the given task, as described by the given configuration.
     * The output collector contains the writers that forward the data to the different tasks that
     * the given task is connected to. Each writer applies the partitioning as described in the
     * configuration.
     *
     * @param task The task that the output collector is created for.
     * @param config The configuration describing the output shipping strategies.
     * @param cl The classloader used to load user defined types.
     * @param eventualOutputs The output writers that this task forwards to the next task for each
     *     output.
     * @param outputOffset The offset to start to get the writers for the outputs
     * @param numOutputs The number of outputs described in the configuration.
     * @return The OutputCollector that data produced in this task is submitted to.
     */
    public static <T> Collector<T> getOutputCollector(
            AbstractInvokable task,
            TaskConfig config,
            ClassLoader cl,
            List<RecordWriter<?>> eventualOutputs,
            int outputOffset,
            int numOutputs)
            throws Exception {
        if (numOutputs == 0) {
            return null;
        }

        // get the factory for the serializer
        final TypeSerializerFactory<T> serializerFactory = config.getOutputSerializer(cl);
        final List<RecordWriter<SerializationDelegate<T>>> writers = new ArrayList<>(numOutputs);

        // create a writer for each output
        for (int i = 0; i < numOutputs; i++) {
            // create the OutputEmitter from output ship strategy
            final ShipStrategyType strategy = config.getOutputShipStrategy(i);
            final int indexInSubtaskGroup = task.getIndexInSubtaskGroup();
            final TypeComparatorFactory<T> compFactory = config.getOutputComparator(i, cl);

            final ChannelSelector<SerializationDelegate<T>> oe;
            if (compFactory == null) {
                oe = new OutputEmitter<>(strategy, indexInSubtaskGroup);
            } else {
                final DataDistribution dataDist = config.getOutputDataDistribution(i, cl);
                final Partitioner<?> partitioner = config.getOutputPartitioner(i, cl);

                final TypeComparator<T> comparator = compFactory.createComparator();
                oe =
                        new OutputEmitter<>(
                                strategy, indexInSubtaskGroup, comparator, partitioner, dataDist);
            }

            final RecordWriter<SerializationDelegate<T>> recordWriter =
                    new RecordWriterBuilder()
                            .setChannelSelector(oe)
                            .setTaskName(task.getEnvironment().getTaskInfo().getTaskName())
                            .build(task.getEnvironment().getWriter(outputOffset + i));

            recordWriter.setMetricGroup(task.getEnvironment().getMetricGroup().getIOMetricGroup());

            writers.add(recordWriter);
        }
        if (eventualOutputs != null) {
            eventualOutputs.addAll(writers);
        }
        return new OutputCollector<>(writers, serializerFactory.getSerializer());
    }

    /**
     * Creates a writer for each output. Creates an OutputCollector which forwards its input to all
     * writers. The output collector applies the configured shipping strategy.
     */
    @SuppressWarnings("unchecked")
    public static <T> Collector<T> initOutputs(
            AbstractInvokable containingTask,
            UserCodeClassLoader cl,
            TaskConfig config,
            List<ChainedDriver<?, ?>> chainedTasksTarget,
            List<RecordWriter<?>> eventualOutputs,
            ExecutionConfig executionConfig,
            Map<String, Accumulator<?, ?>> accumulatorMap)
            throws Exception {
        final int numOutputs = config.getNumOutputs();

        // check whether we got any chained tasks
        final int numChained = config.getNumberOfChainedStubs();
        if (numChained > 0) {
            // got chained stubs. that means that this one may only have a single forward connection
            if (numOutputs != 1 || config.getOutputShipStrategy(0) != ShipStrategyType.FORWARD) {
                throw new RuntimeException(
                        "Plan Generation Bug: Found a chained stub that is not connected via an only forward connection.");
            }

            // instantiate each task
            @SuppressWarnings("rawtypes")
            Collector previous = null;
            for (int i = numChained - 1; i >= 0; --i) {
                // get the task first
                final ChainedDriver<?, ?> ct;
                try {
                    Class<? extends ChainedDriver<?, ?>> ctc = config.getChainedTask(i);
                    ct = ctc.newInstance();
                } catch (Exception ex) {
                    throw new RuntimeException("Could not instantiate chained task driver.", ex);
                }

                // get the configuration for the task
                final TaskConfig chainedStubConf = config.getChainedStubConfig(i);
                final String taskName = config.getChainedTaskName(i);

                if (i == numChained - 1) {
                    // last in chain, instantiate the output collector for this task
                    previous =
                            getOutputCollector(
                                    containingTask,
                                    chainedStubConf,
                                    cl.asClassLoader(),
                                    eventualOutputs,
                                    0,
                                    chainedStubConf.getNumOutputs());
                }

                ct.setup(
                        chainedStubConf,
                        taskName,
                        previous,
                        containingTask,
                        cl,
                        executionConfig,
                        accumulatorMap);
                chainedTasksTarget.add(0, ct);

                if (i == numChained - 1) {
                    ct.getIOMetrics().reuseOutputMetricsForTask();
                }

                previous = ct;
            }
            // the collector of the first in the chain is the collector for the task
            return (Collector<T>) previous;
        }
        // else

        // instantiate the output collector the default way from this configuration
        return getOutputCollector(
                containingTask, config, cl.asClassLoader(), eventualOutputs, 0, numOutputs);
    }

    // --------------------------------------------------------------------------------------------
    //                                  User Code LifeCycle
    // --------------------------------------------------------------------------------------------

    /**
     * Opens the given stub using its {@link
     * org.apache.flink.api.common.functions.RichFunction#open(Configuration)} method. If the open
     * call produces an exception, a new exception with a standard error message is created, using
     * the encountered exception as its cause.
     *
     * @param stub The user code instance to be opened.
     * @param parameters The parameters supplied to the user code.
     * @throws Exception Thrown, if the user code's open method produces an exception.
     */
    public static void openUserCode(Function stub, Configuration parameters) throws Exception {
        try {
            FunctionUtils.openFunction(stub, parameters);
        } catch (Throwable t) {
            throw new Exception(
                    "The user defined 'open(Configuration)' method in "
                            + stub.getClass().toString()
                            + " caused an exception: "
                            + t.getMessage(),
                    t);
        }
    }

    /**
     * Closes the given stub using its {@link
     * org.apache.flink.api.common.functions.RichFunction#close()} method. If the close call
     * produces an exception, a new exception with a standard error message is created, using the
     * encountered exception as its cause.
     *
     * @param stub The user code instance to be closed.
     * @throws Exception Thrown, if the user code's close method produces an exception.
     */
    public static void closeUserCode(Function stub) throws Exception {
        try {
            FunctionUtils.closeFunction(stub);
        } catch (Throwable t) {
            throw new Exception(
                    "The user defined 'close()' method caused an exception: " + t.getMessage(), t);
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
    public static void openChainedTasks(List<ChainedDriver<?, ?>> tasks, AbstractInvokable parent)
            throws Exception {
        // start all chained tasks
        for (ChainedDriver<?, ?> task : tasks) {
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
    public static void closeChainedTasks(List<ChainedDriver<?, ?>> tasks, AbstractInvokable parent)
            throws Exception {
        for (ChainedDriver<?, ?> task : tasks) {
            task.closeTask();

            if (LOG.isDebugEnabled()) {
                LOG.debug(constructLogString("Finished task code", task.getTaskName(), parent));
            }
        }
    }

    /**
     * Cancels all tasks via their {@link ChainedDriver#cancelTask()} method. Any occurring
     * exception and error is suppressed, such that the canceling method of every task is invoked in
     * all cases.
     *
     * @param tasks The tasks to be canceled.
     */
    public static void cancelChainedTasks(List<ChainedDriver<?, ?>> tasks) {
        for (ChainedDriver<?, ?> task : tasks) {
            try {
                task.cancelTask();
            } catch (Throwable t) {
                // do nothing
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //                                     Miscellaneous Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Instantiates a user code class from is definition in the task configuration. The class is
     * instantiated without arguments using the null-ary constructor. Instantiation will fail if
     * this constructor does not exist or is not public.
     *
     * @param <T> The generic type of the user code class.
     * @param config The task configuration containing the class description.
     * @param cl The class loader to be used to load the class.
     * @param superClass The super class that the user code class extends or implements, for type
     *     checking.
     * @return An instance of the user code class.
     */
    public static <T> T instantiateUserCode(
            TaskConfig config, ClassLoader cl, Class<? super T> superClass) {
        try {
            T stub = config.<T>getStubWrapper(cl).getUserCodeObject(superClass, cl);
            // check if the class is a subclass, if the check is required
            if (superClass != null && !superClass.isAssignableFrom(stub.getClass())) {
                throw new RuntimeException(
                        "The class '"
                                + stub.getClass().getName()
                                + "' is not a subclass of '"
                                + superClass.getName()
                                + "' as is required.");
            }
            return stub;
        } catch (ClassCastException ccex) {
            throw new RuntimeException(
                    "The UDF class is not a proper subclass of " + superClass.getName(), ccex);
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

    public static void clearWriters(List<RecordWriter<?>> writers) {
        for (RecordWriter<?> writer : writers) {
            writer.close();
        }
    }

    public static void clearReaders(MutableReader<?>[] readers) {
        for (MutableReader<?> reader : readers) {
            reader.clearBuffers();
        }
    }
}
