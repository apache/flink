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
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.metrics.groups.InternalOperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * DataSourceTask which is executed by a task manager. The task reads data and uses an {@link
 * InputFormat} to create records from the input.
 *
 * @see org.apache.flink.api.common.io.InputFormat
 */
public class DataSourceTask<OT> extends AbstractInvokable {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceTask.class);

    private List<RecordWriter<?>> eventualOutputs;

    // Output collector
    private Collector<OT> output;

    // InputFormat instance
    private InputFormat<OT, InputSplit> format;

    // type serializer for the input
    private TypeSerializerFactory<OT> serializerFactory;

    // Task configuration
    private TaskConfig config;

    // tasks chained to this data source
    private ArrayList<ChainedDriver<?, ?>> chainedTasks;

    // cancel flag
    private volatile boolean taskCanceled = false;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public DataSourceTask(Environment environment) {
        super(environment);
    }

    @Override
    public void invoke() throws Exception {
        // --------------------------------------------------------------------
        // Initialize
        // --------------------------------------------------------------------
        initInputFormat();

        LOG.debug(getLogString("Start registering input and output"));

        try {
            initOutputs(getEnvironment().getUserCodeClassLoader());
        } catch (Exception ex) {
            throw new RuntimeException(
                    "The initialization of the DataSource's outputs caused an error: "
                            + ex.getMessage(),
                    ex);
        }

        LOG.debug(getLogString("Finished registering input and output"));

        // --------------------------------------------------------------------
        // Invoke
        // --------------------------------------------------------------------
        LOG.debug(getLogString("Starting data source operator"));

        RuntimeContext ctx = createRuntimeContext();

        final Counter numRecordsOut;
        {
            Counter tmpNumRecordsOut;
            try {
                InternalOperatorIOMetricGroup ioMetricGroup =
                        ((InternalOperatorMetricGroup) ctx.getMetricGroup()).getIOMetricGroup();
                ioMetricGroup.reuseInputMetricsForTask();
                if (this.config.getNumberOfChainedStubs() == 0) {
                    ioMetricGroup.reuseOutputMetricsForTask();
                }
                tmpNumRecordsOut = ioMetricGroup.getNumRecordsOutCounter();
            } catch (Exception e) {
                LOG.warn("An exception occurred during the metrics setup.", e);
                tmpNumRecordsOut = new SimpleCounter();
            }
            numRecordsOut = tmpNumRecordsOut;
        }

        Counter completedSplitsCounter = ctx.getMetricGroup().counter("numSplitsProcessed");

        if (RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
            ((RichInputFormat) this.format).setRuntimeContext(ctx);
            LOG.debug(getLogString("Rich Source detected. Initializing runtime context."));
            ((RichInputFormat) this.format).openInputFormat();
            LOG.debug(getLogString("Rich Source detected. Opening the InputFormat."));
        }

        ExecutionConfig executionConfig = getExecutionConfig();

        boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        LOG.debug(
                "DataSourceTask object reuse: "
                        + (objectReuseEnabled ? "ENABLED" : "DISABLED")
                        + ".");

        final TypeSerializer<OT> serializer = this.serializerFactory.getSerializer();

        try {
            // start all chained tasks
            BatchTask.openChainedTasks(this.chainedTasks, this);

            // get input splits to read
            final Iterator<InputSplit> splitIterator = getInputSplits();

            // for each assigned input split
            while (!this.taskCanceled && splitIterator.hasNext()) {
                // get start and end
                final InputSplit split = splitIterator.next();

                LOG.debug(getLogString("Opening input split " + split.toString()));

                final InputFormat<OT, InputSplit> format = this.format;

                // open input format
                format.open(split);

                LOG.debug(getLogString("Starting to read input from split " + split.toString()));

                try {
                    final Collector<OT> output =
                            new CountingCollector<>(this.output, numRecordsOut);

                    if (objectReuseEnabled) {
                        OT reuse = serializer.createInstance();

                        // as long as there is data to read
                        while (!this.taskCanceled && !format.reachedEnd()) {

                            OT returned;
                            if ((returned = format.nextRecord(reuse)) != null) {
                                output.collect(returned);
                            }
                        }
                    } else {
                        // as long as there is data to read
                        while (!this.taskCanceled && !format.reachedEnd()) {
                            OT returned;
                            if ((returned = format.nextRecord(serializer.createInstance()))
                                    != null) {
                                output.collect(returned);
                            }
                        }
                    }

                    if (LOG.isDebugEnabled() && !this.taskCanceled) {
                        LOG.debug(getLogString("Closing input split " + split.toString()));
                    }
                } finally {
                    // close. We close here such that a regular close throwing an exception marks a
                    // task as failed.
                    format.close();
                }
                completedSplitsCounter.inc();
            } // end for all input splits

            // close all chained tasks letting them report failure
            BatchTask.closeChainedTasks(this.chainedTasks, this);

            // close the output collector
            this.output.close();
        } catch (Exception ex) {
            // close the input, but do not report any exceptions, since we already have another root
            // cause
            try {
                this.format.close();
            } catch (Throwable ignored) {
            }

            BatchTask.cancelChainedTasks(this.chainedTasks);

            ex = ExceptionInChainedStubException.exceptionUnwrap(ex);

            if (ex instanceof CancelTaskException) {
                // forward canceling exception
                throw ex;
            } else if (!this.taskCanceled) {
                // drop exception, if the task was canceled
                BatchTask.logAndThrowException(ex, this);
            }
        } finally {
            BatchTask.clearWriters(eventualOutputs);
            // --------------------------------------------------------------------
            // Closing
            // --------------------------------------------------------------------
            if (this.format != null
                    && RichInputFormat.class.isAssignableFrom(this.format.getClass())) {
                ((RichInputFormat) this.format).closeInputFormat();
                LOG.debug(getLogString("Rich Source detected. Closing the InputFormat."));
            }
            terminationFuture.complete(null);
        }

        if (!this.taskCanceled) {
            LOG.debug(getLogString("Finished data source operator"));
        } else {
            LOG.debug(getLogString("Data source operator cancelled"));
        }
    }

    @Override
    public Future<Void> cancel() throws Exception {
        this.taskCanceled = true;
        LOG.debug(getLogString("Cancelling data source operator"));
        return terminationFuture;
    }

    /**
     * Initializes the InputFormat implementation and configuration.
     *
     * @throws RuntimeException Throws if instance of InputFormat implementation can not be
     *     obtained.
     */
    private void initInputFormat() {
        ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        // obtain task configuration (including stub parameters)
        Configuration taskConf = getTaskConfiguration();
        this.config = new TaskConfig(taskConf);

        final Pair<OperatorID, InputFormat<OT, InputSplit>> operatorIdAndInputFormat;
        InputOutputFormatContainer formatContainer =
                new InputOutputFormatContainer(config, userCodeClassLoader);
        try {
            operatorIdAndInputFormat = formatContainer.getUniqueInputFormat();
            this.format = operatorIdAndInputFormat.getValue();

            // check if the class is a subclass, if the check is required
            if (!InputFormat.class.isAssignableFrom(this.format.getClass())) {
                throw new RuntimeException(
                        "The class '"
                                + this.format.getClass().getName()
                                + "' is not a subclass of '"
                                + InputFormat.class.getName()
                                + "' as is required.");
            }
        } catch (ClassCastException ccex) {
            throw new RuntimeException(
                    "The stub class is not a proper subclass of " + InputFormat.class.getName(),
                    ccex);
        }

        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        // configure the stub. catch exceptions here extra, to report them as originating from the
        // user code
        try {
            thread.setContextClassLoader(userCodeClassLoader);
            this.format.configure(formatContainer.getParameters(operatorIdAndInputFormat.getKey()));
        } catch (Throwable t) {
            throw new RuntimeException(
                    "The user defined 'configure()' method caused an error: " + t.getMessage(), t);
        } finally {
            thread.setContextClassLoader(original);
        }

        // get the factory for the type serializer
        this.serializerFactory = this.config.getOutputSerializer(userCodeClassLoader);
    }

    /**
     * Creates a writer for each output. Creates an OutputCollector which forwards its input to all
     * writers. The output collector applies the configured shipping strategy.
     */
    private void initOutputs(UserCodeClassLoader cl) throws Exception {
        this.chainedTasks = new ArrayList<ChainedDriver<?, ?>>();
        this.eventualOutputs = new ArrayList<RecordWriter<?>>();

        this.output =
                BatchTask.initOutputs(
                        this,
                        cl,
                        this.config,
                        this.chainedTasks,
                        this.eventualOutputs,
                        getExecutionConfig(),
                        getEnvironment().getAccumulatorRegistry().getUserMap());
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
        return getLogString(message, this.getEnvironment().getTaskInfo().getTaskName());
    }

    /**
     * Utility function that composes a string for logging purposes. The string includes the given
     * message and the index of the task in its task group together with the number of tasks in the
     * task group.
     *
     * @param message The main message for the log.
     * @param taskName The name of the task.
     * @return The string ready for logging.
     */
    private String getLogString(String message, String taskName) {
        return BatchTask.constructLogString(message, taskName, this);
    }

    private Iterator<InputSplit> getInputSplits() {

        final InputSplitProvider provider = getEnvironment().getInputSplitProvider();

        return new Iterator<InputSplit>() {

            private InputSplit nextSplit;

            private boolean exhausted;

            @Override
            public boolean hasNext() {
                if (exhausted) {
                    return false;
                }

                if (nextSplit != null) {
                    return true;
                }

                final InputSplit split;
                try {
                    split = provider.getNextInputSplit(getUserCodeClassLoader());
                } catch (InputSplitProviderException e) {
                    throw new RuntimeException("Could not retrieve next input split.", e);
                }

                if (split != null) {
                    this.nextSplit = split;
                    return true;
                } else {
                    exhausted = true;
                    return false;
                }
            }

            @Override
            public InputSplit next() {
                if (this.nextSplit == null && !hasNext()) {
                    throw new NoSuchElementException();
                }

                final InputSplit tmp = this.nextSplit;
                this.nextSplit = null;
                return tmp;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public DistributedRuntimeUDFContext createRuntimeContext() {
        Environment env = getEnvironment();

        String sourceName = getEnvironment().getTaskInfo().getTaskName().split("->")[0].trim();
        sourceName = sourceName.startsWith("CHAIN") ? sourceName.substring(6) : sourceName;

        return new DistributedRuntimeUDFContext(
                env.getTaskInfo(),
                env.getUserCodeClassLoader(),
                getExecutionConfig(),
                env.getDistributedCacheEntries(),
                env.getAccumulatorRegistry().getUserMap(),
                getEnvironment().getMetricGroup().getOrAddOperator(sourceName),
                env.getExternalResourceInfoProvider(),
                env.getJobID());
    }
}
