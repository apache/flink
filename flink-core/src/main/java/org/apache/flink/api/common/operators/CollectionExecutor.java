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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.common.operators.base.BulkIterationBase.PartialSolutionPlaceHolder;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.api.common.operators.base.DeltaIterationBase.SolutionSetPlaceHolder;
import org.apache.flink.api.common.operators.base.DeltaIterationBase.WorksetPlaceHolder;
import org.apache.flink.api.common.operators.util.TypeComparable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.types.Value;
import org.apache.flink.util.Visitor;

/**
 * Execution utility for serial, local, collection-based executions of Flink programs.
 */
public class CollectionExecutor {
	
	private static final boolean DEFAULT_MUTABLE_OBJECT_SAFE_MODE = true;
	
	private final Map<Operator<?>, List<?>> intermediateResults;
	
	private final Map<String, Accumulator<?, ?>> accumulators;
	
	private final Map<String, Value> previousAggregates;
	
	private final Map<String, Aggregator<?>> aggregators;
	
	private final ClassLoader classLoader;
	
	private final ExecutionConfig executionConfig;

	// --------------------------------------------------------------------------------------------
	
	public CollectionExecutor(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
		
		this.intermediateResults = new HashMap<Operator<?>, List<?>>();
		this.accumulators = new HashMap<String, Accumulator<?,?>>();
		this.previousAggregates = new HashMap<String, Value>();
		this.aggregators = new HashMap<String, Aggregator<?>>();
		
		this.classLoader = getClass().getClassLoader();
	}
	
	// --------------------------------------------------------------------------------------------
	//  General execution methods
	// --------------------------------------------------------------------------------------------
	
	public JobExecutionResult execute(Plan program) throws Exception {
		long startTime = System.currentTimeMillis();
		
		Collection<? extends GenericDataSinkBase<?>> sinks = program.getDataSinks();
		for (Operator<?> sink : sinks) {
			execute(sink);
		}
		
		long endTime = System.currentTimeMillis();
		Map<String, Object> accumulatorResults = AccumulatorHelper.toResultMap(accumulators);
		return new JobExecutionResult(endTime - startTime, accumulatorResults);
	}
	
	private List<?> execute(Operator<?> operator) throws Exception {
		return execute(operator, 0);
	}
	
	private List<?> execute(Operator<?> operator, int superStep) throws Exception {
		List<?> result = this.intermediateResults.get(operator);
		
		// if it has already been computed, use the cached variant
		if (result != null) {
			return result;
		}
		
		if (operator instanceof BulkIterationBase) {
			result = executeBulkIteration((BulkIterationBase<?>) operator);
		}
		else if (operator instanceof DeltaIterationBase) {
			result = executeDeltaIteration((DeltaIterationBase<?, ?>) operator);
		}
		else if (operator instanceof SingleInputOperator) {
			result = executeUnaryOperator((SingleInputOperator<?, ?, ?>) operator, superStep);
		}
		else if (operator instanceof DualInputOperator) {
			result = executeBinaryOperator((DualInputOperator<?, ?, ?, ?>) operator, superStep);
		}
		else if (operator instanceof GenericDataSourceBase) {
			result = executeDataSource((GenericDataSourceBase<?, ?>) operator);
		}
		else if (operator instanceof GenericDataSinkBase) {
			executeDataSink((GenericDataSinkBase<?>) operator);
			result = Collections.emptyList();
		}
		else {
			throw new RuntimeException("Cannot execute operator " + operator.getClass().getName());
		}
		
		this.intermediateResults.put(operator, result);
		
		return result;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Operator class specific execution methods
	// --------------------------------------------------------------------------------------------
	
	private <IN> void executeDataSink(GenericDataSinkBase<?> sink) throws Exception {
		Operator<?> inputOp = sink.getInput();
		if (inputOp == null) {
			throw new InvalidProgramException("The data sink " + sink.getName() + " has no input.");
		}
		
		@SuppressWarnings("unchecked")
		List<IN> input = (List<IN>) execute(inputOp);
		
		@SuppressWarnings("unchecked")
		GenericDataSinkBase<IN> typedSink = (GenericDataSinkBase<IN>) sink;

		typedSink.executeOnCollections(input, executionConfig);
	}
	
	private <OUT> List<OUT> executeDataSource(GenericDataSourceBase<?, ?> source) throws Exception {
		@SuppressWarnings("unchecked")
		GenericDataSourceBase<OUT, ?> typedSource = (GenericDataSourceBase<OUT, ?>) source;
		return typedSource.executeOnCollections(executionConfig);
	}
	
	private <IN, OUT> List<OUT> executeUnaryOperator(SingleInputOperator<?, ?, ?> operator, int superStep) throws Exception {
		Operator<?> inputOp = operator.getInput();
		if (inputOp == null) {
			throw new InvalidProgramException("The unary operation " + operator.getName() + " has no input.");
		}
		
		@SuppressWarnings("unchecked")
		List<IN> inputData = (List<IN>) execute(inputOp, superStep);
		
		@SuppressWarnings("unchecked")
		SingleInputOperator<IN, OUT, ?> typedOp = (SingleInputOperator<IN, OUT, ?>) operator;
		
		// build the runtime context and compute broadcast variables, if necessary
		RuntimeUDFContext ctx;
		if (RichFunction.class.isAssignableFrom(typedOp.getUserCodeWrapper().getUserCodeClass())) {
			ctx = superStep == 0 ? new RuntimeUDFContext(operator.getName(), 1, 0, getClass().getClassLoader(), executionConfig) :
					new IterationRuntimeUDFContext(operator.getName(), 1, 0, superStep, classLoader, executionConfig);
			
			for (Map.Entry<String, Operator<?>> bcInputs : operator.getBroadcastInputs().entrySet()) {
				List<?> bcData = execute(bcInputs.getValue());
				ctx.setBroadcastVariable(bcInputs.getKey(), bcData);
			}
		} else {
			ctx = null;
		}
		
		List<OUT> result = typedOp.executeOnCollections(inputData, ctx, executionConfig);
		
		if (ctx != null) {
			AccumulatorHelper.mergeInto(this.accumulators, ctx.getAllAccumulators());
		}
		return result;
	}
	
	private <IN1, IN2, OUT> List<OUT> executeBinaryOperator(DualInputOperator<?, ?, ?, ?> operator, int superStep) throws Exception {
		Operator<?> inputOp1 = operator.getFirstInput();
		Operator<?> inputOp2 = operator.getSecondInput();
		
		if (inputOp1 == null) {
			throw new InvalidProgramException("The binary operation " + operator.getName() + " has no first input.");
		}
		if (inputOp2 == null) {
			throw new InvalidProgramException("The binary operation " + operator.getName() + " has no second input.");
		}
		
		// compute inputs
		@SuppressWarnings("unchecked")
		List<IN1> inputData1 = (List<IN1>) execute(inputOp1, superStep);
		@SuppressWarnings("unchecked")
		List<IN2> inputData2 = (List<IN2>) execute(inputOp2, superStep);
		
		@SuppressWarnings("unchecked")
		DualInputOperator<IN1, IN2, OUT, ?> typedOp = (DualInputOperator<IN1, IN2, OUT, ?>) operator;
		
		// build the runtime context and compute broadcast variables, if necessary
		RuntimeUDFContext ctx;
		if (RichFunction.class.isAssignableFrom(typedOp.getUserCodeWrapper().getUserCodeClass())) {
			ctx = superStep == 0 ? new RuntimeUDFContext(operator.getName(), 1, 0, classLoader, executionConfig) :
				new IterationRuntimeUDFContext(operator.getName(), 1, 0, superStep, classLoader, executionConfig);
			
			for (Map.Entry<String, Operator<?>> bcInputs : operator.getBroadcastInputs().entrySet()) {
				List<?> bcData = execute(bcInputs.getValue());
				ctx.setBroadcastVariable(bcInputs.getKey(), bcData);
			}
		} else {
			ctx = null;
		}
		
		List<OUT> result = typedOp.executeOnCollections(inputData1, inputData2, ctx, executionConfig);
		
		if (ctx != null) {
			AccumulatorHelper.mergeInto(this.accumulators, ctx.getAllAccumulators());
		}
		return result;
	}
	
	@SuppressWarnings("unchecked")
	private <T> List<T> executeBulkIteration(BulkIterationBase<?> iteration) throws Exception {
		Operator<?> inputOp = iteration.getInput();
		if (inputOp == null) {
			throw new InvalidProgramException("The iteration " + iteration.getName() + " has no input (initial partial solution).");
		}
		if (iteration.getNextPartialSolution() == null) {
			throw new InvalidProgramException("The iteration " + iteration.getName() + " has no next partial solution defined (is not closed).");
		}
		
		List<T> inputData = (List<T>) execute(inputOp);
		
		// get the operators that are iterative
		Set<Operator<?>> dynamics = new LinkedHashSet<Operator<?>>();
		DynamicPathCollector dynCollector = new DynamicPathCollector(dynamics);
		iteration.getNextPartialSolution().accept(dynCollector);
		if (iteration.getTerminationCriterion() != null) {
			iteration.getTerminationCriterion().accept(dynCollector);
		}
		
		// register the aggregators
		for (AggregatorWithName<?> a : iteration.getAggregators().getAllRegisteredAggregators()) {
			aggregators.put(a.getName(), a.getAggregator());
		}
		
		String convCriterionAggName = iteration.getAggregators().getConvergenceCriterionAggregatorName();
		ConvergenceCriterion<Value> convCriterion = (ConvergenceCriterion<Value>) iteration.getAggregators().getConvergenceCriterion();
		
		List<T> currentResult = inputData;
		
		final int maxIterations = iteration.getMaximumNumberOfIterations();
		
		for (int superstep = 1; superstep <= maxIterations; superstep++) {
			
			// set the input to the current partial solution
			this.intermediateResults.put(iteration.getPartialSolution(), currentResult);
			
			// grab the current iteration result
			currentResult = (List<T>) execute(iteration.getNextPartialSolution(), superstep);

			// evaluate the termination criterion
			if (iteration.getTerminationCriterion() != null) {
				execute(iteration.getTerminationCriterion(), superstep);
			}
			
			// evaluate the aggregator convergence criterion
			if (convCriterion != null && convCriterionAggName != null) {
				Value v = aggregators.get(convCriterionAggName).getAggregate();
				if (convCriterion.isConverged(superstep, v)) {
					break;
				}
			}
			
			// clear the dynamic results
			for (Operator<?> o : dynamics) {
				intermediateResults.remove(o);
			}
			
			// set the previous iteration's aggregates and reset the aggregators
			for (Map.Entry<String, Aggregator<?>> e : aggregators.entrySet()) {
				previousAggregates.put(e.getKey(), e.getValue().getAggregate());
				e.getValue().reset();
			}
		}
		
		previousAggregates.clear();
		aggregators.clear();
		
		return currentResult;
	}

	@SuppressWarnings("unchecked")
	private <T> List<T> executeDeltaIteration(DeltaIterationBase<?, ?> iteration) throws Exception {
		Operator<?> solutionInput = iteration.getInitialSolutionSet();
		Operator<?> worksetInput = iteration.getInitialWorkset();
		if (solutionInput == null) {
			throw new InvalidProgramException("The delta iteration " + iteration.getName() + " has no initial solution set.");
		}
		if (worksetInput == null) {
			throw new InvalidProgramException("The delta iteration " + iteration.getName() + " has no initial workset.");
		}
		if (iteration.getSolutionSetDelta() == null) {
			throw new InvalidProgramException("The iteration " + iteration.getName() + " has no solution set delta defined (is not closed).");
		}
		if (iteration.getNextWorkset() == null) {
			throw new InvalidProgramException("The iteration " + iteration.getName() + " has no workset defined (is not closed).");
		}


		List<T> solutionInputData = (List<T>) execute(solutionInput);
		List<T> worksetInputData = (List<T>) execute(worksetInput);

		// get the operators that are iterative
		Set<Operator<?>> dynamics = new LinkedHashSet<Operator<?>>();
		DynamicPathCollector dynCollector = new DynamicPathCollector(dynamics);
		iteration.getSolutionSetDelta().accept(dynCollector);
		iteration.getNextWorkset().accept(dynCollector);

		BinaryOperatorInformation<?, ?, ?> operatorInfo = iteration.getOperatorInfo();
		TypeInformation<?> solutionType = operatorInfo.getFirstInputType();

		int[] keyColumns = iteration.getSolutionSetKeyFields();
		boolean[] inputOrderings = new boolean[keyColumns.length];
		TypeComparator<T> inputComparator = ((CompositeType<T>) solutionType).createComparator(keyColumns, inputOrderings, 0, executionConfig);

		Map<TypeComparable<T>, T> solutionMap = new HashMap<TypeComparable<T>, T>(solutionInputData.size());
		// fill the solution from the initial input
		for (T delta: solutionInputData) {
			TypeComparable<T> wrapper = new TypeComparable<T>(delta, inputComparator);
			solutionMap.put(wrapper, delta);
		}

		List<?> currentWorkset = worksetInputData;

		// register the aggregators
		for (AggregatorWithName<?> a : iteration.getAggregators().getAllRegisteredAggregators()) {
			aggregators.put(a.getName(), a.getAggregator());
		}

		final int maxIterations = iteration.getMaximumNumberOfIterations();

		for (int superstep = 1; superstep <= maxIterations; superstep++) {

			List<T> currentSolution = new ArrayList<T>(solutionMap.size());
			currentSolution.addAll(solutionMap.values());

			// set the input to the current partial solution
			this.intermediateResults.put(iteration.getSolutionSet(), currentSolution);
			this.intermediateResults.put(iteration.getWorkset(), currentWorkset);

			// grab the current iteration result
			List<T> solutionSetDelta = (List<T>) execute(iteration.getSolutionSetDelta(), superstep);
			this.intermediateResults.put(iteration.getSolutionSetDelta(), solutionSetDelta);

			// update the solution
			for (T delta: solutionSetDelta) {
				TypeComparable<T> wrapper = new TypeComparable<T>(delta, inputComparator);
				solutionMap.put(wrapper, delta);
			}

			currentWorkset = (List<?>) execute(iteration.getNextWorkset(), superstep);

			if (currentWorkset.isEmpty()) {
				break;
			}

			// clear the dynamic results
			for (Operator<?> o : dynamics) {
				intermediateResults.remove(o);
			}
			
			// set the previous iteration's aggregates and reset the aggregators
			for (Map.Entry<String, Aggregator<?>> e : aggregators.entrySet()) {
				previousAggregates.put(e.getKey(), e.getValue().getAggregate());
				e.getValue().reset();
			}
		}
		
		previousAggregates.clear();
		aggregators.clear();

		List<T> currentSolution = new ArrayList<T>(solutionMap.size());
		currentSolution.addAll(solutionMap.values());
		return currentSolution;
	}
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	private static final class DynamicPathCollector implements Visitor<Operator<?>> {

		private final Set<Operator<?>> visited = new HashSet<Operator<?>>();
		
		private final Set<Operator<?>> dynamicPathOperations;
		
		public DynamicPathCollector(Set<Operator<?>> dynamicPathOperations) {
			this.dynamicPathOperations = dynamicPathOperations;
		}

		@Override
		public boolean preVisit(Operator<?> op) {
			return visited.add(op);
		}

		@Override
		public void postVisit(Operator<?> op) {
			
			if (op instanceof SingleInputOperator) {
				SingleInputOperator<?, ?, ?> siop = (SingleInputOperator<?, ?, ?>) op;
				
				if (dynamicPathOperations.contains(siop.getInput())) {
					dynamicPathOperations.add(op);
				} else {
					for (Operator<?> o : siop.getBroadcastInputs().values()) {
						if (dynamicPathOperations.contains(o)) {
							dynamicPathOperations.add(op);
							break;
						}
					}
				}
			}
			else if (op instanceof DualInputOperator) {
				DualInputOperator<?, ?, ?, ?> siop = (DualInputOperator<?, ?, ?, ?>) op;
				
				if (dynamicPathOperations.contains(siop.getFirstInput())) {
					dynamicPathOperations.add(op);
				} else if (dynamicPathOperations.contains(siop.getSecondInput())) {
					dynamicPathOperations.add(op);
				} else {
					for (Operator<?> o : siop.getBroadcastInputs().values()) {
						if (dynamicPathOperations.contains(o)) {
							dynamicPathOperations.add(op);
							break;
						}
					}
				}
			}
			else if (op.getClass() == PartialSolutionPlaceHolder.class ||
				op.getClass() == WorksetPlaceHolder.class ||
				op.getClass() ==  SolutionSetPlaceHolder.class)
			{
				dynamicPathOperations.add(op);
			}
			else if (op instanceof GenericDataSourceBase) {
				// skip
			}
			else {
				throw new RuntimeException("Cannot handle operator type " + op.getClass().getName());
			}
		}
	}
	
	private class IterationRuntimeUDFContext extends RuntimeUDFContext implements IterationRuntimeContext {

		private final int superstep;

		public IterationRuntimeUDFContext(String name, int numParallelSubtasks, int subtaskIndex, int superstep, ClassLoader classloader, ExecutionConfig executionConfig) {
			super(name, numParallelSubtasks, subtaskIndex, classloader, executionConfig);
			this.superstep = superstep;
		}

		@Override
		public int getSuperstepNumber() {
			return superstep;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Aggregator<?>> T getIterationAggregator(String name) {
			return (T) aggregators.get(name);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Value> T getPreviousIterationAggregate(String name) {
			return (T) previousAggregates.get(name);
		}
	}
}
