/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.api.common.operators.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.ConvergenceCriterion;
import eu.stratosphere.api.common.accumulators.SimpleAccumulator;
import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericCollectorMap;
import eu.stratosphere.api.common.operators.IterationOperator;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.OperatorInformation;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Nothing;
import eu.stratosphere.types.NothingTypeInfo;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Visitor;

/**
 * 
 */
public class BulkIterationBase<T> extends SingleInputOperator<T, T, AbstractFunction> implements IterationOperator {
	
	private static String DEFAULT_NAME = "<Unnamed Bulk Iteration>";
	
	public static final String TERMINATION_CRITERION_ACCUMULATOR_NAME = "terminationCriterion.accumulator";
	
	
	private Operator<T> iterationResult;
	
	private final Operator<T> inputPlaceHolder;
	
	private int numberOfIterations = -1;
	
	protected Operator<?> terminationCriterion;
	
	private ConvergenceCriterion<?> convergenceCriterion;
	
	private String convergenceCriterionAccumulatorName;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * 
	 */
	public BulkIterationBase(UnaryOperatorInformation<T, T> operatorInfo) {
		this(operatorInfo, DEFAULT_NAME);
	}
	
	/**
	 * @param name
	 */
	public BulkIterationBase(UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeClassWrapper<AbstractFunction>(AbstractFunction.class), operatorInfo, name);
		inputPlaceHolder = new PartialSolutionPlaceHolder<T>(this, this.getOperatorInfo());
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * @return The operator representing the partial solution.
	 */
	public Operator<T> getPartialSolution() {
		return this.inputPlaceHolder;
	}
	
	/**
	 * @param result
	 */
	public void setNextPartialSolution(Operator<T> result) {
		if (result == null) {
			throw new NullPointerException("Operator producing the next partial solution must not be null.");
		}
		this.iterationResult = result;
	}
	
	/**
	 * @return The operator representing the next partial solution.
	 */
	public Operator<T> getNextPartialSolution() {
		return this.iterationResult;
	}
	
	/**
	 * @return The operator representing the termination criterion.
	 */
	public Operator<?> getTerminationCriterion() {
		return this.terminationCriterion;
	}
	
	/**
	 * @param criterion
	 */
	public <X> void setTerminationCriterion(Operator<X> criterion) {
		CollectorMapOperatorBase<X, Nothing, TerminationCriterionMapper<X>> mapper =
				new CollectorMapOperatorBase<X, Nothing, TerminationCriterionMapper<X>>(
						new TerminationCriterionMapper<X>(),
						new UnaryOperatorInformation<X, Nothing>(criterion.getOperatorInfo().getOutputType(), new NothingTypeInfo()),
						"Termination Criterion Aggregation Wrapper");
		mapper.setInput(criterion);
		
		this.terminationCriterion = mapper;
		this.registerConvergenceCriterion(TERMINATION_CRITERION_ACCUMULATOR_NAME, new TerminationCriterionAccumulationConvergence());
	}
	
	/**
	 * Registers a ConvergenceCriterion on this BulkIteration.
	 * 
	 * @param nameOfAccumulator
	 * @param convergenceCheck
	 */
	public void registerConvergenceCriterion(String nameOfAccumulator, ConvergenceCriterion<?> convergenceCheck)
	{
		this.convergenceCriterionAccumulatorName = nameOfAccumulator;
		this.convergenceCriterion = convergenceCheck;
	}
	
	/**
	 * @param num
	 */
	public void setMaximumNumberOfIterations(int num) {
		if (num < 1) {
			throw new IllegalArgumentException("The number of iterations must be at least one.");
		}
		this.numberOfIterations = num;
	}
	
	public int getMaximumNumberOfIterations() {
		return this.numberOfIterations;
	}
	
	public ConvergenceCriterion<?> getConvergenceCriterion() {
		return convergenceCriterion;
	}

	public String getConvergenceCriterionAccumulatorName() {
		return convergenceCriterionAccumulatorName;
	}

	/**
	 * @throws Exception
	 */
	public void validate() throws InvalidProgramException {
		if (this.input == null) {
			throw new RuntimeException("Operator for initial partial solution is not set.");
		}
		if (this.iterationResult == null) {
			throw new InvalidProgramException("Operator producing the next version of the partial " +
					"solution (iteration result) is not set.");
		}
		if (this.terminationCriterion == null && this.numberOfIterations <= 0) {
			throw new InvalidProgramException("No termination condition is set " +
					"(neither fix number of iteration nor termination criterion).");
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
	 * The BulkIteration meta operator cannot have broadcast inputs.
	 * This method always throws an exception.
	 * 
	 * @param name Ignored.
	 * @param root Ignored.
	 */
	public void setBroadcastVariable(String name, Operator<?> root) {
		throw new UnsupportedOperationException("The BulkIteration meta operator cannot have broadcast inputs.");
	}
	
	/**
	 * The BulkIteration meta operator cannot have broadcast inputs.
	 * This method always throws an exception.
	 * 
	 * @param inputs Ignored
	 */
	public <X> void setBroadcastVariables(Map<String, Operator<X>> inputs) {
		throw new UnsupportedOperationException("The BulkIteration meta operator cannot have broadcast inputs.");
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Specialized operator to use as a recognizable place-holder for the input to the
	 * step function when composing the nested data flow.
	 */
	public static class PartialSolutionPlaceHolder<OT> extends Operator<OT> {
		
		private final BulkIterationBase<OT> containingIteration;
		
		public PartialSolutionPlaceHolder(BulkIterationBase<OT> container, OperatorInformation<OT> operatorInfo) {
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
	 * Special Mapper that is added before a termination criterion and is only a container for an special aggregator
	 */
	public static class TerminationCriterionMapper<X> extends AbstractFunction implements Serializable, GenericCollectorMap<X, Nothing> {
		private static final long serialVersionUID = 1L;
		
		private TerminationCriterionAccumulator accumulator;
		
		@Override
		public void open(Configuration parameters) {
			accumulator = new TerminationCriterionAccumulator();
			getIterationRuntimeContext().addIterationAccumulator(TERMINATION_CRITERION_ACCUMULATOR_NAME, accumulator);
		}
		
		@Override
		public void map(X in, Collector<Nothing> out) {
			accumulator.add(1L);
		}
	}
	
	/**
	 * Aggregator that basically only adds 1 for every output tuple of the termination criterion branch
	 */
	public static class TerminationCriterionAccumulator implements SimpleAccumulator<Long> {

		private static final long serialVersionUID = 1L;
		private long count;

		@Override
		public void add(Long count) {
			this.count += count.longValue();
		}

		@Override
		public Long getLocalValue() {
			return new Long(count);
		}

		@Override
		public void resetLocal() {
			count = 0;
		}

		@Override
		public void merge(Accumulator<Long, Long> other) {
			this.count += other.getLocalValue().longValue();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(count);
		}

		@Override
		public void read(DataInput in) throws IOException {
			this.count = in.readLong();
		}
	}

	/**
	 * Convergence for the termination criterion is reached if no tuple is output at current iteration for the termination criterion branch
	 */
	public static class TerminationCriterionAccumulationConvergence implements ConvergenceCriterion<Long> {

		private static final long serialVersionUID = 1L;
		private static final Log log = LogFactory.getLog(TerminationCriterionAccumulationConvergence.class);

		@Override
		public boolean isConverged(int iteration, Long countAggregate) {
			long count = countAggregate.longValue();

			if (log.isInfoEnabled()) {
				log.info("Termination criterion stats in iteration [" + iteration + "]: " + count);
			}

			return (count == 0);
		}
	}
}
