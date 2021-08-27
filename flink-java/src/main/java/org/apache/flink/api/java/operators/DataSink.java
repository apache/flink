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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;

/**
 * An operation that allows storing data results.
 *
 * @param <T>
 */
@Public
public class DataSink<T> {

    private final OutputFormat<T> format;

    private final TypeInformation<T> type;

    private final DataSet<T> data;

    private String name;

    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    private ResourceSpec minResources = ResourceSpec.DEFAULT;

    private ResourceSpec preferredResources = ResourceSpec.DEFAULT;

    private Configuration parameters;

    private int[] sortKeyPositions;

    private Order[] sortOrders;

    public DataSink(DataSet<T> data, OutputFormat<T> format, TypeInformation<T> type) {
        if (format == null) {
            throw new IllegalArgumentException("The output format must not be null.");
        }
        if (type == null) {
            throw new IllegalArgumentException("The input type information must not be null.");
        }
        if (data == null) {
            throw new IllegalArgumentException("The data set must not be null.");
        }

        this.format = format;
        this.data = data;
        this.type = type;
    }

    @Internal
    public OutputFormat<T> getFormat() {
        return format;
    }

    @Internal
    public TypeInformation<T> getType() {
        return type;
    }

    @Internal
    public DataSet<T> getDataSet() {
        return data;
    }

    /**
     * Pass a configuration to the OutputFormat.
     *
     * @param parameters Configuration parameters
     */
    public DataSink<T> withParameters(Configuration parameters) {
        this.parameters = parameters;
        return this;
    }

    /**
     * Sorts each local partition of a {@link org.apache.flink.api.java.tuple.Tuple} data set on the
     * specified field in the specified {@link Order} before it is emitted by the output format.
     *
     * <p><b>Note: Only tuple data sets can be sorted using integer field indices.</b>
     *
     * <p>The tuple data set can be sorted on multiple fields in different orders by chaining {@link
     * #sortLocalOutput(int, Order)} calls.
     *
     * @param field The Tuple field on which the data set is locally sorted.
     * @param order The Order in which the specified Tuple field is locally sorted.
     * @return This data sink operator with specified output order.
     * @see org.apache.flink.api.java.tuple.Tuple
     * @see Order
     * @deprecated Use {@link DataSet#sortPartition(int, Order)} instead
     */
    @Deprecated
    @PublicEvolving
    public DataSink<T> sortLocalOutput(int field, Order order) {

        // get flat keys
        Keys.ExpressionKeys<T> ek = new Keys.ExpressionKeys<>(field, this.type);
        int[] flatKeys = ek.computeLogicalKeyPositions();

        if (!Keys.ExpressionKeys.isSortKey(field, this.type)) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }

        if (this.sortKeyPositions == null) {
            // set sorting info
            this.sortKeyPositions = flatKeys;
            this.sortOrders = new Order[flatKeys.length];
            Arrays.fill(this.sortOrders, order);
        } else {
            // append sorting info to existing info
            int oldLength = this.sortKeyPositions.length;
            int newLength = oldLength + flatKeys.length;
            this.sortKeyPositions = Arrays.copyOf(this.sortKeyPositions, newLength);
            this.sortOrders = Arrays.copyOf(this.sortOrders, newLength);

            for (int i = 0; i < flatKeys.length; i++) {
                this.sortKeyPositions[oldLength + i] = flatKeys[i];
                this.sortOrders[oldLength + i] = order;
            }
        }

        return this;
    }

    /**
     * Sorts each local partition of a data set on the field(s) specified by the field expression in
     * the specified {@link Order} before it is emitted by the output format.
     *
     * <p><b>Note: Non-composite types can only be sorted on the full element which is specified by
     * a wildcard expression ("*" or "_").</b>
     *
     * <p>Data sets of composite types (Tuple or Pojo) can be sorted on multiple fields in different
     * orders by chaining {@link #sortLocalOutput(String, Order)} calls.
     *
     * @param fieldExpression The field expression for the field(s) on which the data set is locally
     *     sorted.
     * @param order The Order in which the specified field(s) are locally sorted.
     * @return This data sink operator with specified output order.
     * @see Order
     * @deprecated Use {@link DataSet#sortPartition(String, Order)} instead
     */
    @Deprecated
    @PublicEvolving
    public DataSink<T> sortLocalOutput(String fieldExpression, Order order) {

        int numFields;
        int[] fields;
        Order[] orders;

        // compute flat field positions for (nested) sorting fields
        Keys.ExpressionKeys<T> ek = new Keys.ExpressionKeys<>(fieldExpression, this.type);
        fields = ek.computeLogicalKeyPositions();

        if (!Keys.ExpressionKeys.isSortKey(fieldExpression, this.type)) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }

        numFields = fields.length;
        orders = new Order[numFields];
        Arrays.fill(orders, order);

        if (this.sortKeyPositions == null) {
            // set sorting info
            this.sortKeyPositions = fields;
            this.sortOrders = orders;
        } else {
            // append sorting info to existing info
            int oldLength = this.sortKeyPositions.length;
            int newLength = oldLength + numFields;
            this.sortKeyPositions = Arrays.copyOf(this.sortKeyPositions, newLength);
            this.sortOrders = Arrays.copyOf(this.sortOrders, newLength);
            for (int i = 0; i < numFields; i++) {
                this.sortKeyPositions[oldLength + i] = fields[i];
                this.sortOrders[oldLength + i] = orders[i];
            }
        }

        return this;
    }

    /** @return Configuration for the OutputFormat. */
    public Configuration getParameters() {
        return this.parameters;
    }

    // --------------------------------------------------------------------------------------------

    public DataSink<T> name(String name) {
        this.name = name;
        return this;
    }

    // --------------------------------------------------------------------------------------------

    protected GenericDataSinkBase<T> translateToDataFlow(Operator<T> input) {
        // select the name (or create a default one)
        String name = this.name != null ? this.name : this.format.toString();
        GenericDataSinkBase<T> sink =
                new GenericDataSinkBase<>(
                        this.format,
                        new UnaryOperatorInformation<>(this.type, new NothingTypeInfo()),
                        name);
        // set input
        sink.setInput(input);
        // set parameters
        if (this.parameters != null) {
            sink.getParameters().addAll(this.parameters);
        }
        // set parallelism
        if (this.parallelism > 0) {
            // use specified parallelism
            sink.setParallelism(this.parallelism);
        } else {
            // if no parallelism has been specified, use parallelism of input operator to enable
            // chaining
            sink.setParallelism(input.getParallelism());
        }

        if (this.sortKeyPositions != null) {
            // configure output sorting
            Ordering ordering = new Ordering();
            for (int i = 0; i < this.sortKeyPositions.length; i++) {
                ordering.appendOrdering(this.sortKeyPositions[i], null, this.sortOrders[i]);
            }
            sink.setLocalOrder(ordering);
        }

        return sink;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "DataSink '"
                + (this.name == null ? "<unnamed>" : this.name)
                + "' ("
                + this.format.toString()
                + ")";
    }

    /**
     * Returns the parallelism of this data sink.
     *
     * @return The parallelism of this data sink.
     */
    public int getParallelism() {
        return this.parallelism;
    }

    /**
     * Sets the parallelism for this data sink. The degree must be 1 or more.
     *
     * @param parallelism The parallelism for this data sink. A value equal to {@link
     *     ExecutionConfig#PARALLELISM_DEFAULT} will use the system default.
     * @return This data sink with set parallelism.
     */
    public DataSink<T> setParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism);

        this.parallelism = parallelism;

        return this;
    }

    /**
     * Returns the minimum resources of this data sink. If no minimum resources have been set, this
     * returns the default resource profile.
     *
     * @return The minimum resources of this data sink.
     */
    @PublicEvolving
    public ResourceSpec getMinResources() {
        return this.minResources;
    }

    /**
     * Returns the preferred resources of this data sink. If no preferred resources have been set,
     * this returns the default resource profile.
     *
     * @return The preferred resources of this data sink.
     */
    @PublicEvolving
    public ResourceSpec getPreferredResources() {
        return this.preferredResources;
    }

    //	---------------------------------------------------------------------------
    //	 Fine-grained resource profiles are an incomplete work-in-progress feature
    //	 The setters are hence private at this point.
    //	---------------------------------------------------------------------------

    /**
     * Sets the minimum and preferred resources for this data sink. and the lower and upper resource
     * limits will be considered in resource resize feature for future plan.
     *
     * @param minResources The minimum resources for this data sink.
     * @param preferredResources The preferred resources for this data sink.
     * @return The data sink with set minimum and preferred resources.
     */
    private DataSink<T> setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
        OperatorValidationUtils.validateMinAndPreferredResources(minResources, preferredResources);

        this.minResources = minResources;
        this.preferredResources = preferredResources;

        return this;
    }

    /**
     * Sets the resources for this data sink, and the minimum and preferred resources are the same
     * by default.
     *
     * @param resources The resources for this data sink.
     * @return The data sink with set minimum and preferred resources.
     */
    private DataSink<T> setResources(ResourceSpec resources) {
        OperatorValidationUtils.validateResources(resources);

        this.minResources = resources;
        this.preferredResources = resources;

        return this;
    }
}
