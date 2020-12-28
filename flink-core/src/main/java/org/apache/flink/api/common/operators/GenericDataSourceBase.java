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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract superclass for data sources in a Pact plan.
 *
 * @param <OUT> The output type of the data source
 * @param <T> The type of input format invoked by instances of this data source.
 */
@Internal
public class GenericDataSourceBase<OUT, T extends InputFormat<OUT, ?>> extends Operator<OUT> {

    private static final String DEFAULT_NAME = "<Unnamed Generic Data Source>";

    protected final UserCodeWrapper<? extends T> formatWrapper;

    protected String statisticsKey;

    private SplitDataProperties splitProperties;

    /**
     * Creates a new instance for the given file using the given input format.
     *
     * @param format The {@link org.apache.flink.api.common.io.InputFormat} implementation used to
     *     read the data.
     * @param operatorInfo The type information for the operator.
     * @param name The given name for the Pact, used in plans, logs and progress messages.
     */
    public GenericDataSourceBase(T format, OperatorInformation<OUT> operatorInfo, String name) {
        super(operatorInfo, name);

        if (format == null) {
            throw new IllegalArgumentException("Input format may not be null.");
        }

        this.formatWrapper = new UserCodeObjectWrapper<T>(format);
    }

    /**
     * Creates a new instance for the given file using the given input format, using the default
     * name.
     *
     * @param format The {@link org.apache.flink.api.common.io.InputFormat} implementation used to
     *     read the data.
     * @param operatorInfo The type information for the operator.
     */
    public GenericDataSourceBase(T format, OperatorInformation<OUT> operatorInfo) {
        super(operatorInfo, DEFAULT_NAME);

        if (format == null) {
            throw new IllegalArgumentException("Input format may not be null.");
        }

        this.formatWrapper = new UserCodeObjectWrapper<T>(format);
    }

    /**
     * Creates a new instance for the given file using the given input format.
     *
     * @param format The {@link org.apache.flink.api.common.io.InputFormat} implementation used to
     *     read the data.
     * @param operatorInfo The type information for the operator.
     * @param name The given name for the Pact, used in plans, logs and progress messages.
     */
    public GenericDataSourceBase(
            Class<? extends T> format, OperatorInformation<OUT> operatorInfo, String name) {
        super(operatorInfo, name);

        if (format == null) {
            throw new IllegalArgumentException("Input format may not be null.");
        }

        this.formatWrapper = new UserCodeClassWrapper<T>(format);
    }

    /**
     * Creates a new instance for the given file using the given input format, using the default
     * name.
     *
     * @param format The {@link org.apache.flink.api.common.io.InputFormat} implementation used to
     *     read the data.
     * @param operatorInfo The type information for the operator.
     */
    public GenericDataSourceBase(Class<? extends T> format, OperatorInformation<OUT> operatorInfo) {
        super(operatorInfo, DEFAULT_NAME);

        if (format == null) {
            throw new IllegalArgumentException("Input format may not be null.");
        }

        this.formatWrapper = new UserCodeClassWrapper<T>(format);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the class describing the input format.
     *
     * @return The class describing the input format.
     */
    public UserCodeWrapper<? extends T> getFormatWrapper() {
        return this.formatWrapper;
    }

    /**
     * Gets the class describing the input format.
     *
     * <p>This method is basically identical to {@link #getFormatWrapper()}.
     *
     * @return The class describing the input format.
     * @see org.apache.flink.api.common.operators.Operator#getUserCodeWrapper()
     */
    @Override
    public UserCodeWrapper<? extends T> getUserCodeWrapper() {
        return this.formatWrapper;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the key under which statistics about this data source may be obtained from the
     * statistics cache.
     *
     * @return The statistics cache key.
     */
    public String getStatisticsKey() {
        return this.statisticsKey;
    }

    /**
     * Sets the key under which statistics about this data source may be obtained from the
     * statistics cache. Useful for testing purposes, when providing mock statistics.
     *
     * @param statisticsKey The key for the statistics object.
     */
    public void setStatisticsKey(String statisticsKey) {
        this.statisticsKey = statisticsKey;
    }

    /**
     * Sets properties of input splits for this data source. Split properties can help to generate
     * more efficient execution plans. <br>
     * <b> IMPORTANT: Providing wrong split data properties can cause wrong results! </b>
     *
     * @param splitDataProperties The data properties of this data source's splits.
     */
    public void setSplitDataProperties(SplitDataProperties<OUT> splitDataProperties) {
        this.splitProperties = splitDataProperties;
    }

    /**
     * Returns the data properties of this data source's splits.
     *
     * @return The data properties of this data source's splits or null if no properties have been
     *     set.
     */
    public SplitDataProperties<OUT> getSplitDataProperties() {
        return this.splitProperties;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Accepts the visitor and applies it this instance. Since the data sources have no inputs, no
     * recursive descend happens. The visitors pre-visit method is called and, if returning
     * <tt>true</tt>, the post-visit method is called.
     *
     * @param visitor The visitor.
     * @see org.apache.flink.util.Visitable#accept(org.apache.flink.util.Visitor)
     */
    @Override
    public void accept(Visitor<Operator<?>> visitor) {
        if (visitor.preVisit(this)) {
            visitor.postVisit(this);
        }
    }

    // --------------------------------------------------------------------------------------------

    protected List<OUT> executeOnCollections(RuntimeContext ctx, ExecutionConfig executionConfig)
            throws Exception {
        @SuppressWarnings("unchecked")
        InputFormat<OUT, InputSplit> inputFormat =
                (InputFormat<OUT, InputSplit>) this.formatWrapper.getUserCodeObject();
        // configure the input format
        inputFormat.configure(this.parameters);

        // open the input format
        if (inputFormat instanceof RichInputFormat) {
            ((RichInputFormat) inputFormat).setRuntimeContext(ctx);
            ((RichInputFormat) inputFormat).openInputFormat();
        }

        List<OUT> result = new ArrayList<OUT>();

        // splits
        InputSplit[] splits = inputFormat.createInputSplits(1);
        TypeSerializer<OUT> serializer =
                getOperatorInfo().getOutputType().createSerializer(executionConfig);

        for (InputSplit split : splits) {
            inputFormat.open(split);

            while (!inputFormat.reachedEnd()) {
                OUT next = inputFormat.nextRecord(serializer.createInstance());
                if (next != null) {
                    result.add(serializer.copy(next));
                }
            }

            inputFormat.close();
        }

        // close the input format
        if (inputFormat instanceof RichInputFormat) {
            ((RichInputFormat) inputFormat).closeInputFormat();
        }

        return result;
    }

    // --------------------------------------------------------------------------------------------

    public String toString() {
        return this.name;
    }

    public static interface SplitDataProperties<T> {

        public int[] getSplitPartitionKeys();

        public Partitioner<T> getSplitPartitioner();

        public int[] getSplitGroupKeys();

        public Ordering getSplitOrder();
    }
}
