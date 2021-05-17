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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.operators.util.CloseableInputProvider;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * DataSinkTask which is executed by a task manager. The task hands the data to an output format.
 *
 * @see OutputFormat
 */
public class DataSinkTask<IT> extends AbstractInvokable {

    // Obtain DataSinkTask Logger
    private static final Logger LOG = LoggerFactory.getLogger(DataSinkTask.class);

    // --------------------------------------------------------------------------------------------

    // OutputFormat instance. volatile, because the asynchronous canceller may access it
    private volatile OutputFormat<IT> format;

    private MutableReader<?> inputReader;

    // input reader
    private MutableObjectIterator<IT> reader;

    // The serializer for the input type
    private TypeSerializerFactory<IT> inputTypeSerializerFactory;

    // local strategy
    private CloseableInputProvider<IT> localStrategy;

    // task configuration
    private TaskConfig config;

    // cancel flag
    private volatile boolean taskCanceled;

    private volatile boolean cleanupCalled;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public DataSinkTask(Environment environment) {
        super(environment);
    }

    @Override
    public void invoke() throws Exception {
        // --------------------------------------------------------------------
        // Initialize
        // --------------------------------------------------------------------
        LOG.debug(getLogString("Start registering input and output"));

        // initialize OutputFormat
        initOutputFormat();

        // initialize input readers
        try {
            initInputReaders();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Initializing the input streams failed"
                            + (e.getMessage() == null ? "." : ": " + e.getMessage()),
                    e);
        }

        LOG.debug(getLogString("Finished registering input and output"));

        // --------------------------------------------------------------------
        // Invoke
        // --------------------------------------------------------------------
        LOG.debug(getLogString("Starting data sink operator"));

        RuntimeContext ctx = createRuntimeContext();

        final Counter numRecordsIn;
        {
            Counter tmpNumRecordsIn;
            try {
                OperatorIOMetricGroup ioMetricGroup =
                        ((OperatorMetricGroup) ctx.getMetricGroup()).getIOMetricGroup();
                ioMetricGroup.reuseInputMetricsForTask();
                ioMetricGroup.reuseOutputMetricsForTask();
                tmpNumRecordsIn = ioMetricGroup.getNumRecordsInCounter();
            } catch (Exception e) {
                LOG.warn("An exception occurred during the metrics setup.", e);
                tmpNumRecordsIn = new SimpleCounter();
            }
            numRecordsIn = tmpNumRecordsIn;
        }

        if (RichOutputFormat.class.isAssignableFrom(this.format.getClass())) {
            ((RichOutputFormat) this.format).setRuntimeContext(ctx);
            LOG.debug(getLogString("Rich Sink detected. Initializing runtime context."));
        }

        ExecutionConfig executionConfig = getExecutionConfig();

        boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        try {
            // initialize local strategies
            MutableObjectIterator<IT> input1;
            switch (this.config.getInputLocalStrategy(0)) {
                case NONE:
                    // nothing to do
                    localStrategy = null;
                    input1 = reader;
                    break;
                case SORT:
                    // initialize sort local strategy
                    try {
                        // get type comparator
                        TypeComparatorFactory<IT> compFact =
                                this.config.getInputComparator(0, getUserCodeClassLoader());
                        if (compFact == null) {
                            throw new Exception(
                                    "Missing comparator factory for local strategy on input " + 0);
                        }

                        // initialize sorter
                        Sorter<IT> sorter =
                                ExternalSorter.newBuilder(
                                                getEnvironment().getMemoryManager(),
                                                this,
                                                this.inputTypeSerializerFactory.getSerializer(),
                                                compFact.createComparator())
                                        .maxNumFileHandles(this.config.getFilehandlesInput(0))
                                        .enableSpilling(
                                                getEnvironment().getIOManager(),
                                                this.config.getSpillingThresholdInput(0))
                                        .memoryFraction(this.config.getRelativeMemoryInput(0))
                                        .objectReuse(
                                                this.getExecutionConfig().isObjectReuseEnabled())
                                        .largeRecords(this.config.getUseLargeRecordHandler())
                                        .build(this.reader);
                        this.localStrategy = sorter;
                        input1 = sorter.getIterator();
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Initializing the input processing failed"
                                        + (e.getMessage() == null ? "." : ": " + e.getMessage()),
                                e);
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid local strategy for DataSinkTask");
            }

            // read the reader and write it to the output

            final TypeSerializer<IT> serializer = this.inputTypeSerializerFactory.getSerializer();
            final MutableObjectIterator<IT> input = input1;
            final OutputFormat<IT> format = this.format;

            // check if task has been canceled
            if (this.taskCanceled) {
                return;
            }

            LOG.debug(getLogString("Starting to produce output"));

            // open
            format.open(
                    this.getEnvironment().getTaskInfo().getIndexOfThisSubtask(),
                    this.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks());

            if (objectReuseEnabled) {
                IT record = serializer.createInstance();

                // work!
                while (!this.taskCanceled && ((record = input.next(record)) != null)) {
                    numRecordsIn.inc();
                    format.writeRecord(record);
                }
            } else {
                IT record;

                // work!
                while (!this.taskCanceled && ((record = input.next()) != null)) {
                    numRecordsIn.inc();
                    format.writeRecord(record);
                }
            }

            // close. We close here such that a regular close throwing an exception marks a task as
            // failed.
            if (!this.taskCanceled) {
                this.format.close();
                this.format = null;
            }
        } catch (Exception ex) {

            // make a best effort to clean up
            try {
                if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
                    cleanupCalled = true;
                    ((CleanupWhenUnsuccessful) format).tryCleanupOnError();
                }
            } catch (Throwable t) {
                LOG.error("Cleanup on error failed.", t);
            }

            ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

            if (ex instanceof CancelTaskException) {
                // forward canceling exception
                throw ex;
            }
            // drop, if the task was canceled
            else if (!this.taskCanceled) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(getLogString("Error in user code: " + ex.getMessage()), ex);
                }
                throw ex;
            }
        } finally {
            if (this.format != null) {
                // close format, if it has not been closed, yet.
                // This should only be the case if we had a previous error, or were canceled.
                try {
                    this.format.close();
                } catch (Throwable t) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn(getLogString("Error closing the output format"), t);
                    }
                }
            }
            // close local strategy if necessary
            if (localStrategy != null) {
                try {
                    this.localStrategy.close();
                } catch (Throwable t) {
                    LOG.error("Error closing local strategy", t);
                }
            }

            BatchTask.clearReaders(new MutableReader<?>[] {inputReader});
            terminationFuture.complete(null);
        }

        if (!this.taskCanceled) {
            LOG.debug(getLogString("Finished data sink operator"));
        } else {
            LOG.debug(getLogString("Data sink operator cancelled"));
        }
    }

    @Override
    public Future<Void> cancel() throws Exception {
        this.taskCanceled = true;
        OutputFormat<IT> format = this.format;
        if (format != null) {
            try {
                this.format.close();
            } catch (Throwable t) {
            }

            // make a best effort to clean up
            try {
                if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
                    cleanupCalled = true;
                    ((CleanupWhenUnsuccessful) format).tryCleanupOnError();
                }
            } catch (Throwable t) {
                LOG.error("Cleanup on error failed.", t);
            }
        }

        LOG.debug(getLogString("Cancelling data sink operator"));
        return terminationFuture;
    }

    /**
     * Initializes the OutputFormat implementation and configuration.
     *
     * @throws RuntimeException Throws if instance of OutputFormat implementation can not be
     *     obtained.
     */
    private void initOutputFormat() {
        ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        // obtain task configuration (including stub parameters)
        Configuration taskConf = getTaskConfiguration();
        this.config = new TaskConfig(taskConf);

        final Pair<OperatorID, OutputFormat<IT>> operatorIDAndOutputFormat;
        InputOutputFormatContainer formatContainer =
                new InputOutputFormatContainer(config, userCodeClassLoader);
        try {
            operatorIDAndOutputFormat = formatContainer.getUniqueOutputFormat();
            this.format = operatorIDAndOutputFormat.getValue();

            // check if the class is a subclass, if the check is required
            if (!OutputFormat.class.isAssignableFrom(this.format.getClass())) {
                throw new RuntimeException(
                        "The class '"
                                + this.format.getClass().getName()
                                + "' is not a subclass of '"
                                + OutputFormat.class.getName()
                                + "' as is required.");
            }
        } catch (ClassCastException ccex) {
            throw new RuntimeException(
                    "The stub class is not a proper subclass of " + OutputFormat.class.getName(),
                    ccex);
        }

        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        // configure the stub. catch exceptions here extra, to report them as originating from the
        // user code
        try {
            thread.setContextClassLoader(userCodeClassLoader);
            this.format.configure(
                    formatContainer.getParameters(operatorIDAndOutputFormat.getKey()));
        } catch (Throwable t) {
            throw new RuntimeException(
                    "The user defined 'configure()' method in the Output Format caused an error: "
                            + t.getMessage(),
                    t);
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    /**
     * Initializes the input readers of the DataSinkTask.
     *
     * @throws RuntimeException Thrown in case of invalid task input configuration.
     */
    @SuppressWarnings("unchecked")
    private void initInputReaders() throws Exception {
        int numGates = 0;
        //  ---------------- create the input readers ---------------------
        // in case where a logical input unions multiple physical inputs, create a union reader
        final int groupSize = this.config.getGroupSize(0);
        numGates += groupSize;
        if (groupSize == 1) {
            // non-union case
            inputReader =
                    new MutableRecordReader<DeserializationDelegate<IT>>(
                            getEnvironment().getInputGate(0),
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());
        } else if (groupSize > 1) {
            // union case
            inputReader =
                    new MutableRecordReader<IOReadableWritable>(
                            new UnionInputGate(getEnvironment().getAllInputGates()),
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());
        } else {
            throw new Exception("Illegal input group size in task configuration: " + groupSize);
        }

        this.inputTypeSerializerFactory =
                this.config.getInputSerializer(0, getUserCodeClassLoader());
        @SuppressWarnings({"rawtypes"})
        final MutableObjectIterator<?> iter =
                new ReaderIterator(inputReader, this.inputTypeSerializerFactory.getSerializer());
        this.reader = (MutableObjectIterator<IT>) iter;

        // final sanity check
        if (numGates != this.config.getNumInputs()) {
            throw new Exception(
                    "Illegal configuration: Number of input gates and group sizes are not consistent.");
        }
    }

    // ------------------------------------------------------------------------
    //                               Utilities
    // ------------------------------------------------------------------------

    /**
     * Utility function that composes a string for logging purposes. The string includes the given
     * message and the index of the task in its task group together with the number of tasks in the
     * task group.
     *
     * @param message The main message for the log.
     * @return The string ready for logging.
     */
    private String getLogString(String message) {
        return BatchTask.constructLogString(
                message, this.getEnvironment().getTaskInfo().getTaskName(), this);
    }

    public DistributedRuntimeUDFContext createRuntimeContext() {
        Environment env = getEnvironment();

        return new DistributedRuntimeUDFContext(
                env.getTaskInfo(),
                env.getUserCodeClassLoader(),
                getExecutionConfig(),
                env.getDistributedCacheEntries(),
                env.getAccumulatorRegistry().getUserMap(),
                getEnvironment()
                        .getMetricGroup()
                        .getOrAddOperator(getEnvironment().getTaskInfo().getTaskName()),
                env.getExternalResourceInfoProvider(),
                env.getJobID());
    }
}
