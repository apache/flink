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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.ResettableDriver;
import org.apache.flink.runtime.operators.TaskContext;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class DriverTestBase<S extends Function> implements TaskContext<S, Record> {

    protected static final long DEFAULT_PER_SORT_MEM = 16 * 1024 * 1024;

    protected static final int PAGE_SIZE = 32 * 1024;

    private final IOManager ioManager;

    private final MemoryManager memManager;

    private final List<MutableObjectIterator<Record>> inputs;

    private final List<TypeComparator<Record>> comparators;

    private final List<Sorter<Record>> sorters;

    private final AbstractInvokable owner;

    private final TaskConfig taskConfig;

    private final TaskManagerRuntimeInfo taskManageInfo;

    protected final long perSortMem;

    protected final double perSortFractionMem;

    private Collector<Record> output;

    protected int numFileHandles;

    private S stub;

    private Driver<S, Record> driver;

    private volatile boolean running = true;

    private ExecutionConfig executionConfig;

    protected DriverTestBase(ExecutionConfig executionConfig, long memory, int maxNumSorters) {
        this(executionConfig, memory, maxNumSorters, DEFAULT_PER_SORT_MEM);
    }

    protected DriverTestBase(
            ExecutionConfig executionConfig, long memory, int maxNumSorters, long perSortMemory) {
        if (memory < 0 || maxNumSorters < 0 || perSortMemory < 0) {
            throw new IllegalArgumentException();
        }

        final long totalMem = Math.max(memory, 0) + (Math.max(maxNumSorters, 0) * perSortMemory);

        this.perSortMem = perSortMemory;
        this.perSortFractionMem = (double) perSortMemory / totalMem;
        this.ioManager = new IOManagerAsync();
        this.memManager =
                totalMem > 0
                        ? MemoryManagerBuilder.newBuilder().setMemorySize(totalMem).build()
                        : null;

        this.inputs = new ArrayList<>();
        this.comparators = new ArrayList<>();
        this.sorters = new ArrayList<>();

        this.owner = new DummyInvokable();
        this.taskConfig = new TaskConfig(new Configuration());
        this.executionConfig = executionConfig;
        this.taskManageInfo = new TestingTaskManagerRuntimeInfo();
    }

    @Parameters
    private static Collection<Object[]> getConfigurations() {

        LinkedList<Object[]> configs = new LinkedList<Object[]>();

        ExecutionConfig withReuse = new ExecutionConfig();
        withReuse.enableObjectReuse();

        ExecutionConfig withoutReuse = new ExecutionConfig();
        withoutReuse.disableObjectReuse();

        Object[] a = {withoutReuse};
        configs.add(a);
        Object[] b = {withReuse};
        configs.add(b);

        return configs;
    }

    protected void addInput(MutableObjectIterator<Record> input) {
        this.inputs.add(input);
        this.sorters.add(null);
    }

    protected void addInputSorted(MutableObjectIterator<Record> input, RecordComparator comp)
            throws Exception {
        Sorter<Record> sorter =
                ExternalSorter.newBuilder(
                                this.memManager,
                                this.owner,
                                RecordSerializerFactory.get().getSerializer(),
                                comp)
                        .maxNumFileHandles(32)
                        .enableSpilling(ioManager, 0.8f)
                        .memoryFraction(this.perSortFractionMem)
                        .objectReuse(true)
                        .largeRecords(true)
                        .build(input);
        this.sorters.add(sorter);
        this.inputs.add(null);
    }

    protected void addDriverComparator(RecordComparator comparator) {
        this.comparators.add(comparator);
    }

    protected void setOutput(Collector<Record> output) {
        this.output = output;
    }

    protected void setOutput(List<Record> output) {
        this.output = new ListOutputCollector(output);
    }

    protected int getNumFileHandlesForSort() {
        return numFileHandles;
    }

    protected void setNumFileHandlesForSort(int numFileHandles) {
        this.numFileHandles = numFileHandles;
    }

    @SuppressWarnings("rawtypes")
    protected void testDriver(Driver driver, Class stubClass) throws Exception {
        testDriverInternal(driver, stubClass);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void testDriverInternal(Driver driver, Class stubClass) throws Exception {

        this.driver = driver;
        driver.setup(this);

        this.stub = (S) stubClass.newInstance();

        // regular running logic
        boolean stubOpen = false;

        try {
            // run the data preparation
            try {
                driver.prepare();
            } catch (Throwable t) {
                throw new Exception("The data preparation caused an error: " + t.getMessage(), t);
            }

            // open stub implementation
            try {
                FunctionUtils.openFunction(this.stub, DefaultOpenContext.INSTANCE);
                stubOpen = true;
            } catch (Throwable t) {
                throw new Exception(
                        "The user defined 'open()' method caused an exception: " + t.getMessage(),
                        t);
            }

            if (!running) {
                return;
            }

            // run the user code
            driver.run();

            // close. We close here such that a regular close throwing an exception marks a task as
            // failed.
            if (this.running) {
                FunctionUtils.closeFunction(this.stub);
                stubOpen = false;
            }

            this.output.close();
        } catch (Exception ex) {
            // close the input, but do not report any exceptions, since we already have another root
            // cause
            if (stubOpen) {
                try {
                    FunctionUtils.closeFunction(this.stub);
                } catch (Throwable ignored) {
                }
            }

            // if resettable driver invoke tear down
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

            // drop exception, if the task was canceled
            if (this.running) {
                throw ex;
            }

        } finally {
            driver.cleanup();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void testResettableDriver(ResettableDriver driver, Class stubClass, int iterations)
            throws Exception {

        driver.setup(this);

        for (int i = 0; i < iterations; i++) {

            if (i == 0) {
                driver.initialize();
            } else {
                driver.reset();
            }

            testDriver(driver, stubClass);
        }

        driver.teardown();
    }

    protected void cancel() throws Exception {
        this.running = false;

        // compensate for races, where cancel is called before the driver is set
        // note that this is an artifact of a bad design of this test base, where the setup
        // of the basic properties is not separated from the invocation of the execution logic
        while (this.driver == null) {
            Thread.sleep(200);
        }
        this.driver.cancel();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public TaskConfig getTaskConfig() {
        return this.taskConfig;
    }

    @Override
    public TaskManagerRuntimeInfo getTaskManagerInfo() {
        return this.taskManageInfo;
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return getClass().getClassLoader();
    }

    @Override
    public IOManager getIOManager() {
        return this.ioManager;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return this.memManager;
    }

    @Override
    public <X> MutableObjectIterator<X> getInput(int index) {
        MutableObjectIterator<Record> in = this.inputs.get(index);
        if (in == null) {
            // waiting from sorter
            try {
                in = this.sorters.get(index).getIterator();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted");
            } catch (IOException e) {
                throw new RuntimeException("IOException");
            }
            this.inputs.set(index, in);
        }

        @SuppressWarnings("unchecked")
        MutableObjectIterator<X> input = (MutableObjectIterator<X>) this.inputs.get(index);
        return input;
    }

    @Override
    public <X> TypeSerializerFactory<X> getInputSerializer(int index) {
        @SuppressWarnings("unchecked")
        TypeSerializerFactory<X> factory = (TypeSerializerFactory<X>) RecordSerializerFactory.get();
        return factory;
    }

    @Override
    public <X> TypeComparator<X> getDriverComparator(int index) {
        @SuppressWarnings("unchecked")
        TypeComparator<X> comparator = (TypeComparator<X>) this.comparators.get(index);
        return comparator;
    }

    @Override
    public S getStub() {
        return this.stub;
    }

    @Override
    public Collector<Record> getOutputCollector() {
        return this.output;
    }

    @Override
    public AbstractInvokable getContainingTask() {
        return this.owner;
    }

    @Override
    public String formatLogString(String message) {
        return "Driver Tester: " + message;
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
    }

    // --------------------------------------------------------------------------------------------

    @AfterEach
    void shutdownAll() throws Exception {
        // 1st, shutdown sorters
        for (Sorter<?> sorter : this.sorters) {
            if (sorter != null) {
                sorter.close();
            }
        }
        this.sorters.clear();

        // 2nd, shutdown I/O
        this.ioManager.close();

        // last, verify all memory is returned and shutdown mem manager
        MemoryManager memMan = getMemoryManager();
        if (memMan != null) {
            assertThat(memMan.verifyEmpty())
                    .withFailMessage("Memory Manager managed memory was not completely freed.")
                    .isTrue();
            memMan.shutdown();
        }
    }

    // --------------------------------------------------------------------------------------------

    private static final class ListOutputCollector implements Collector<Record> {

        private final List<Record> output;

        public ListOutputCollector(List<Record> outputList) {
            this.output = outputList;
        }

        @Override
        public void collect(Record record) {
            this.output.add(record.createCopy());
        }

        @Override
        public void close() {}
    }

    public static final class CountingOutputCollector implements Collector<Record> {

        private int num;

        @Override
        public void collect(Record record) {
            this.num++;
        }

        @Override
        public void close() {}

        public int getNumberOfRecords() {
            return this.num;
        }
    }
}
