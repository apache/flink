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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorRegistry;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.IterationOperator;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Visitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** */
@Internal
public class BulkIterationBase<T> extends SingleInputOperator<T, T, AbstractRichFunction>
        implements IterationOperator {

    private static final String DEFAULT_NAME = "<Unnamed Bulk Iteration>";

    public static final String TERMINATION_CRITERION_AGGREGATOR_NAME =
            "terminationCriterion.aggregator";

    private Operator<T> iterationResult;

    private final Operator<T> inputPlaceHolder;

    private final AggregatorRegistry aggregators = new AggregatorRegistry();

    private int numberOfIterations = -1;

    protected Operator<?> terminationCriterion;

    // --------------------------------------------------------------------------------------------

    /** */
    public BulkIterationBase(UnaryOperatorInformation<T, T> operatorInfo) {
        this(operatorInfo, DEFAULT_NAME);
    }

    /** @param name */
    public BulkIterationBase(UnaryOperatorInformation<T, T> operatorInfo, String name) {
        super(
                new UserCodeClassWrapper<AbstractRichFunction>(AbstractRichFunction.class),
                operatorInfo,
                name);
        inputPlaceHolder = new PartialSolutionPlaceHolder<T>(this, this.getOperatorInfo());
    }

    // --------------------------------------------------------------------------------------------

    /** @return The operator representing the partial solution. */
    public Operator<T> getPartialSolution() {
        return this.inputPlaceHolder;
    }

    /** @param result */
    public void setNextPartialSolution(Operator<T> result) {
        if (result == null) {
            throw new NullPointerException(
                    "Operator producing the next partial solution must not be null.");
        }
        this.iterationResult = result;
    }

    /** @return The operator representing the next partial solution. */
    public Operator<T> getNextPartialSolution() {
        return this.iterationResult;
    }

    /** @return The operator representing the termination criterion. */
    public Operator<?> getTerminationCriterion() {
        return this.terminationCriterion;
    }

    /** @param criterion */
    public <X> void setTerminationCriterion(Operator<X> criterion) {

        TypeInformation<X> type = criterion.getOperatorInfo().getOutputType();

        FlatMapOperatorBase<X, X, TerminationCriterionMapper<X>> mapper =
                new FlatMapOperatorBase<X, X, TerminationCriterionMapper<X>>(
                        new TerminationCriterionMapper<X>(),
                        new UnaryOperatorInformation<X, X>(type, type),
                        "Termination Criterion Aggregation Wrapper");
        mapper.setInput(criterion);

        this.terminationCriterion = mapper;
        this.getAggregators()
                .registerAggregationConvergenceCriterion(
                        TERMINATION_CRITERION_AGGREGATOR_NAME,
                        new TerminationCriterionAggregator(),
                        new TerminationCriterionAggregationConvergence());
    }

    /** @param num */
    public void setMaximumNumberOfIterations(int num) {
        if (num < 1) {
            throw new IllegalArgumentException("The number of iterations must be at least one.");
        }
        this.numberOfIterations = num;
    }

    public int getMaximumNumberOfIterations() {
        return this.numberOfIterations;
    }

    @Override
    public AggregatorRegistry getAggregators() {
        return this.aggregators;
    }

    /** @throws InvalidProgramException */
    public void validate() throws InvalidProgramException {
        if (this.input == null) {
            throw new RuntimeException("Operator for initial partial solution is not set.");
        }
        if (this.iterationResult == null) {
            throw new InvalidProgramException(
                    "Operator producing the next version of the partial "
                            + "solution (iteration result) is not set.");
        }
        if (this.terminationCriterion == null && this.numberOfIterations <= 0) {
            throw new InvalidProgramException(
                    "No termination condition is set "
                            + "(neither fix number of iteration nor termination criterion).");
        }
    }

    /**
     * The BulkIteration meta operator cannot have broadcast inputs.
     *
     * @return An empty map.
     */
    public Map<String, Operator<?>> getBroadcastInputs() {
        return Collections.emptyMap();
    }

    /**
     * The BulkIteration meta operator cannot have broadcast inputs. This method always throws an
     * exception.
     *
     * @param name Ignored.
     * @param root Ignored.
     */
    public void setBroadcastVariable(String name, Operator<?> root) {
        throw new UnsupportedOperationException(
                "The BulkIteration meta operator cannot have broadcast inputs.");
    }

    /**
     * The BulkIteration meta operator cannot have broadcast inputs. This method always throws an
     * exception.
     *
     * @param inputs Ignored
     */
    public <X> void setBroadcastVariables(Map<String, Operator<X>> inputs) {
        throw new UnsupportedOperationException(
                "The BulkIteration meta operator cannot have broadcast inputs.");
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Specialized operator to use as a recognizable place-holder for the input to the step function
     * when composing the nested data flow.
     */
    public static class PartialSolutionPlaceHolder<OT> extends Operator<OT> {

        private final BulkIterationBase<OT> containingIteration;

        public PartialSolutionPlaceHolder(
                BulkIterationBase<OT> container, OperatorInformation<OT> operatorInfo) {
            super(operatorInfo, "Partial Solution");
            this.containingIteration = container;
        }

        public BulkIterationBase<OT> getContainingBulkIteration() {
            return this.containingIteration;
        }

        @Override
        public void accept(Visitor<Operator<?>> visitor) {
            visitor.preVisit(this);
            visitor.postVisit(this);
        }

        @Override
        public UserCodeWrapper<?> getUserCodeWrapper() {
            return null;
        }
    }

    /**
     * Special Mapper that is added before a termination criterion and is only a container for an
     * special aggregator
     */
    public static class TerminationCriterionMapper<X> extends AbstractRichFunction
            implements FlatMapFunction<X, X> {
        private static final long serialVersionUID = 1L;

        private TerminationCriterionAggregator aggregator;

        @Override
        public void open(OpenContext openContext) {
            aggregator =
                    getIterationRuntimeContext()
                            .getIterationAggregator(TERMINATION_CRITERION_AGGREGATOR_NAME);
        }

        @Override
        public void flatMap(X in, Collector<X> out) {
            aggregator.aggregate(1L);
        }
    }

    /**
     * Aggregator that basically only adds 1 for every output tuple of the termination criterion
     * branch
     */
    @SuppressWarnings("serial")
    public static class TerminationCriterionAggregator implements Aggregator<LongValue> {

        private long count;

        @Override
        public LongValue getAggregate() {
            return new LongValue(count);
        }

        public void aggregate(long count) {
            this.count += count;
        }

        @Override
        public void aggregate(LongValue count) {
            this.count += count.getValue();
        }

        @Override
        public void reset() {
            count = 0;
        }
    }

    /**
     * Convergence for the termination criterion is reached if no tuple is output at current
     * iteration for the termination criterion branch
     */
    public static class TerminationCriterionAggregationConvergence
            implements ConvergenceCriterion<LongValue> {

        private static final long serialVersionUID = 1L;

        private static final Logger log =
                LoggerFactory.getLogger(TerminationCriterionAggregationConvergence.class);

        @Override
        public boolean isConverged(int iteration, LongValue countAggregate) {
            long count = countAggregate.getValue();

            if (log.isInfoEnabled()) {
                log.info("Termination criterion stats in iteration [" + iteration + "]: " + count);
            }

            return count == 0;
        }
    }

    @Override
    protected List<T> executeOnCollections(
            List<T> inputData, RuntimeContext runtimeContext, ExecutionConfig executionConfig) {
        throw new UnsupportedOperationException();
    }
}
