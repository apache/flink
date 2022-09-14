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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.types.Nothing;
import org.apache.flink.util.Visitor;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Operator for nodes that act as data sinks, storing the data they receive. The way the data is
 * stored is handled by the {@link org.apache.flink.api.common.io.OutputFormat}.
 */
@Internal
public class GenericDataSinkBase<IN> extends Operator<Nothing> {

    protected final UserCodeWrapper<? extends OutputFormat<IN>> formatWrapper;

    protected Operator<IN> input = null;

    private Ordering localOrdering;

    // --------------------------------------------------------------------------------------------

    /**
     * Creates a GenericDataSink with the provided {@link
     * org.apache.flink.api.common.io.OutputFormat} implementation and the given name.
     *
     * @param f The {@link org.apache.flink.api.common.io.OutputFormat} implementation used to sink
     *     the data.
     * @param name The given name for the sink, used in plans, logs and progress messages.
     */
    public GenericDataSinkBase(
            OutputFormat<IN> f, UnaryOperatorInformation<IN, Nothing> operatorInfo, String name) {
        super(operatorInfo, name);

        checkNotNull(f, "The OutputFormat may not be null.");
        this.formatWrapper = new UserCodeObjectWrapper<OutputFormat<IN>>(f);
    }

    /**
     * Creates a GenericDataSink with the provided {@link
     * org.apache.flink.api.common.io.OutputFormat} implementation and the given name.
     *
     * @param f The {@link org.apache.flink.api.common.io.OutputFormat} implementation used to sink
     *     the data.
     * @param name The given name for the sink, used in plans, logs and progress messages.
     */
    public GenericDataSinkBase(
            UserCodeWrapper<? extends OutputFormat<IN>> f,
            UnaryOperatorInformation<IN, Nothing> operatorInfo,
            String name) {
        super(operatorInfo, name);
        this.formatWrapper = checkNotNull(f, "The OutputFormat class may not be null.");
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Returns this operator's input operator.
     *
     * @return This operator's input.
     */
    public Operator<IN> getInput() {
        return this.input;
    }

    /**
     * Sets the given operator as the input to this operator.
     *
     * @param input The operator to use as the input.
     */
    public void setInput(Operator<IN> input) {
        this.input = checkNotNull(input, "The input may not be null.");
    }

    /**
     * Sets the input to the union of the given operators.
     *
     * @param inputs The operator(s) that form the input.
     * @deprecated This method will be removed in future versions. Use the {@link
     *     org.apache.flink.api.common.operators.Union} operator instead.
     */
    @Deprecated
    public void setInputs(Operator<IN>... inputs) {
        checkNotNull(inputs, "The inputs may not be null.");
        this.input = Operator.createUnionCascade(inputs);
    }

    /**
     * Sets the input to the union of the given operators.
     *
     * @param inputs The operator(s) that form the input.
     * @deprecated This method will be removed in future versions. Use the {@link
     *     org.apache.flink.api.common.operators.Union} operator instead.
     */
    @Deprecated
    public void setInputs(List<Operator<IN>> inputs) {
        checkNotNull(inputs, "The inputs may not be null.");
        this.input = Operator.createUnionCascade(inputs);
    }

    /**
     * Adds to the input the union of the given operators.
     *
     * @param inputs The operator(s) to be unioned with the input.
     * @deprecated This method will be removed in future versions. Use the {@link
     *     org.apache.flink.api.common.operators.Union} operator instead.
     */
    @Deprecated
    public void addInput(Operator<IN>... inputs) {
        checkNotNull(inputs, "The input may not be null.");
        this.input = Operator.createUnionCascade(this.input, inputs);
    }

    /**
     * Adds to the input the union of the given operators.
     *
     * @param inputs The operator(s) to be unioned with the input.
     * @deprecated This method will be removed in future versions. Use the {@link
     *     org.apache.flink.api.common.operators.Union} operator instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public void addInputs(List<? extends Operator<IN>> inputs) {
        checkNotNull(inputs, "The inputs may not be null.");
        this.input =
                createUnionCascade(
                        this.input, (Operator<IN>[]) inputs.toArray(new Operator[inputs.size()]));
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the order, in which the data sink writes its data locally. Local order means that with
     * in each fragment of the file inside the distributed file system, the data is ordered, but not
     * across file fragments.
     *
     * @return NONE, if the sink writes data in any order, or ASCENDING (resp. DESCENDING), if the
     *     sink writes it data with a local ascending (resp. descending) order.
     */
    public Ordering getLocalOrder() {
        return this.localOrdering;
    }

    /**
     * Sets the order in which the sink must write its data within each fragment in the distributed
     * file system. For any value other then <tt>NONE</tt>, this will cause the system to perform a
     * local sort, or try to reuse an order from a previous operation.
     *
     * @param localOrder The local order to write the data in.
     */
    public void setLocalOrder(Ordering localOrder) {
        this.localOrdering = localOrder;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the class describing this sinks output format.
     *
     * @return The output format class.
     */
    public UserCodeWrapper<? extends OutputFormat<IN>> getFormatWrapper() {
        return this.formatWrapper;
    }

    /**
     * Gets the class describing the output format.
     *
     * <p>This method is basically identical to {@link #getFormatWrapper()}.
     *
     * @return The class describing the output format.
     * @see org.apache.flink.api.common.operators.Operator#getUserCodeWrapper()
     */
    @Override
    public UserCodeWrapper<? extends OutputFormat<IN>> getUserCodeWrapper() {
        return this.formatWrapper;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Accepts the visitor and applies it this instance. This method applies the visitor in a
     * depth-first traversal. The visitors pre-visit method is called and, if returning
     * <tt>true</tt>, the visitor is recursively applied on the single input. After the recursion
     * returned, the post-visit method is called.
     *
     * @param visitor The visitor.
     * @see org.apache.flink.util.Visitable#accept(org.apache.flink.util.Visitor)
     */
    @Override
    public void accept(Visitor<Operator<?>> visitor) {
        boolean descend = visitor.preVisit(this);
        if (descend) {
            this.input.accept(visitor);
            visitor.postVisit(this);
        }
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    protected void executeOnCollections(
            List<IN> inputData, RuntimeContext ctx, ExecutionConfig executionConfig)
            throws Exception {
        OutputFormat<IN> format = this.formatWrapper.getUserCodeObject();
        TypeInformation<IN> inputType = getInput().getOperatorInfo().getOutputType();

        if (this.localOrdering != null) {
            int[] sortColumns = this.localOrdering.getFieldPositions();
            boolean[] sortOrderings = this.localOrdering.getFieldSortDirections();

            final TypeComparator<IN> sortComparator;
            if (inputType instanceof CompositeType) {
                sortComparator =
                        ((CompositeType<IN>) inputType)
                                .createComparator(sortColumns, sortOrderings, 0, executionConfig);
            } else if (inputType instanceof AtomicType) {
                sortComparator =
                        ((AtomicType<IN>) inputType)
                                .createComparator(sortOrderings[0], executionConfig);
            } else {
                throw new UnsupportedOperationException(
                        "Local output sorting does not support type " + inputType + " yet.");
            }

            Collections.sort(
                    inputData,
                    new Comparator<IN>() {
                        @Override
                        public int compare(IN o1, IN o2) {
                            return sortComparator.compare(o1, o2);
                        }
                    });
        }

        if (format instanceof InitializeOnMaster) {
            ((InitializeOnMaster) format).initializeGlobal(1);
        }
        format.configure(this.parameters);

        if (format instanceof RichOutputFormat) {
            ((RichOutputFormat<?>) format).setRuntimeContext(ctx);
        }
        format.open(0, 1);
        for (IN element : inputData) {
            format.writeRecord(element);
        }

        format.close();

        if (format instanceof FinalizeOnMaster) {
            ((FinalizeOnMaster) format).finalizeGlobal(1);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return this.name;
    }
}
