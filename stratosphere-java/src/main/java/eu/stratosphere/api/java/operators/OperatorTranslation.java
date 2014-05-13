/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.common.operators.AbstractUdfOperator;
import eu.stratosphere.api.common.operators.BinaryOperatorInformation;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.BulkIterationBase;
import eu.stratosphere.api.common.operators.base.DeltaIterationBase;
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase;
import eu.stratosphere.api.java.BulkIterationResultSet;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.DeltaIterationResultSet;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.operators.translation.JavaPlan;
import eu.stratosphere.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class OperatorTranslation {
	
	/** The already translated operations */
	private Map<DataSet<?>, Operator<?>> translated = new HashMap<DataSet<?>, Operator<?>>();
	
	
	public JavaPlan translateToPlan(List<DataSink<?>> sinks, String jobName) {
		List<GenericDataSinkBase<?>> planSinks = new ArrayList<GenericDataSinkBase<?>>();
		
		for (DataSink<?> sink : sinks) {
			planSinks.add(translate(sink));
		}
		
		return new JavaPlan(planSinks); 
	}
	
	
	private <T> GenericDataSinkBase<T> translate(DataSink<T> sink) {
		
		// translate the input recursively
		Operator<T> input = translate(sink.getDataSet());
		
		// translate the sink itself and connect it to the input
		GenericDataSinkBase<T> translatedSink = sink.translateToDataFlow(input);
				
		return translatedSink;
	}
	
	
	private <T> Operator<T> translate(DataSet<T> dataSet) {
		// check if we have already translated that data set (operation or source)
		Operator<?> previous = (Operator<?>) this.translated.get(dataSet);
		if (previous != null) {
			@SuppressWarnings("unchecked")
			Operator<T> typedPrevious = (Operator<T>) previous;
			return typedPrevious;
		}
		
		Operator<T> dataFlowOp;
		
		if (dataSet instanceof DataSource) {
			dataFlowOp = ((DataSource<T>) dataSet).translateToDataFlow();
		}
		else if (dataSet instanceof SingleInputOperator) {
			dataFlowOp = translateSingleInputOperator((SingleInputOperator<?, ?, ?>) dataSet);
		}
		else if (dataSet instanceof TwoInputOperator) {
			dataFlowOp = translateTwoInputOperator((TwoInputOperator<?, ?, ?, ?>) dataSet);
		}
		else if (dataSet instanceof BulkIterationResultSet) {
			dataFlowOp = translateBulkIteration((BulkIterationResultSet<?>) dataSet);
		}
		else if (dataSet instanceof DeltaIterationResultSet) {
			dataFlowOp = translateDeltaIteration((DeltaIterationResultSet<?, ?>) dataSet);
		}
		else {
			throw new RuntimeException("Error while creating the data flow plan for the program: Unknown operator or data set type: " + dataSet);
		}
		
		this.translated.put(dataSet, dataFlowOp);
		
		// take care of broadcast variables
		translateBcVariables(dataSet, dataFlowOp);
		
		return dataFlowOp;
	}
	
	
	private <I, O> eu.stratosphere.api.common.operators.SingleInputOperator<?, O, ?> translateSingleInputOperator(SingleInputOperator<?, ?, ?> op) {
		
		@SuppressWarnings("unchecked")
		SingleInputOperator<I, O, ?> typedOp = (SingleInputOperator<I, O, ?>) op;
		
		@SuppressWarnings("unchecked")
		DataSet<I> typedInput = (DataSet<I>) op.getInput();
		
		Operator<I> input = translate(typedInput);
		
		eu.stratosphere.api.common.operators.SingleInputOperator<?, O, ?> dataFlowOp = typedOp.translateToDataFlow(input);
		
		if (op instanceof UdfOperator<?> ) {
			@SuppressWarnings("unchecked")
			SingleInputUdfOperator<I, O, ?> udfOp = (SingleInputUdfOperator<I, O, ?>) op;
			
			// set configuration parameters
			Configuration opParams = udfOp.getParameters();
			if (opParams != null) {
				dataFlowOp.getParameters().addAll(opParams);
			}
			
			// set the semantic properties
			dataFlowOp.setSemanticProperties(udfOp.getSematicProperties());
		}
		
		return dataFlowOp;
	}
	
	private <I1, I2, O> eu.stratosphere.api.common.operators.DualInputOperator<?, ?, O, ?> translateTwoInputOperator(TwoInputOperator<?, ?, ?, ?> op) {
		
		@SuppressWarnings("unchecked")
		TwoInputOperator<I1, I2, O, ?> typedOp = (TwoInputOperator<I1, I2, O, ?>) op;
		
		@SuppressWarnings("unchecked")
		DataSet<I1> typedInput1 = (DataSet<I1>) op.getInput1();
		@SuppressWarnings("unchecked")
		DataSet<I2> typedInput2 = (DataSet<I2>) op.getInput2();
		
		Operator<I1> input1 = translate(typedInput1);
		Operator<I2> input2 = translate(typedInput2);
		
		eu.stratosphere.api.common.operators.DualInputOperator<?, ?, O, ?> dataFlowOp = typedOp.translateToDataFlow(input1, input2);
		
		if (op instanceof UdfOperator<?> ) {
			@SuppressWarnings("unchecked")
			TwoInputUdfOperator<I1, I2, O, ?> udfOp = (TwoInputUdfOperator<I1, I2, O, ?>) op;
			
			// set configuration parameters
			Configuration opParams = udfOp.getParameters();
			if (opParams != null) {
				dataFlowOp.getParameters().addAll(opParams);
			}
			
			// set the semantic properties
			dataFlowOp.setSemanticProperties(udfOp.getSematicProperties());
		}
		
		return dataFlowOp;
	}
	
	
	private <T> BulkIterationBase<T> translateBulkIteration(BulkIterationResultSet<?> untypedIterationEnd) {
		@SuppressWarnings("unchecked")
		BulkIterationResultSet<T> iterationEnd = (BulkIterationResultSet<T>) untypedIterationEnd;
		
		BulkIterationBase<T> iterationOperator =
				new BulkIterationBase<T>(new UnaryOperatorInformation<T, T>(iterationEnd.getType(), iterationEnd.getType()), "Bulk Iteration");
		IterativeDataSet<T> iterationHead = iterationEnd.getIterationHead();

		translated.put(iterationHead, iterationOperator.getPartialSolution());

		Operator<T> translatedBody = translate(iterationEnd.getNextPartialSolution());
		iterationOperator.setNextPartialSolution(translatedBody);
		iterationOperator.setMaximumNumberOfIterations(iterationHead.getMaxIterations());
		iterationOperator.setInput(translate(iterationHead.getInput()));
		
		iterationOperator.getAggregators().addAll(iterationHead.getAggregators());
		
		if(iterationEnd.getTerminationCriterion() != null) {
			iterationOperator.setTerminationCriterion(translate(iterationEnd.getTerminationCriterion()));
		}

		return iterationOperator;
	}
	
	private <D, W> DeltaIterationBase<D, W> translateDeltaIteration(DeltaIterationResultSet<?, ?> untypedIterationEnd) {
		@SuppressWarnings("unchecked")
		DeltaIterationResultSet<D, W> iterationEnd = (DeltaIterationResultSet<D, W>) untypedIterationEnd;
		
		DeltaIterationBase<D, W> iterationOperator = new DeltaIterationBase<D, W>(new BinaryOperatorInformation<D, W, D>(iterationEnd.getType(), iterationEnd.getWorksetType(), iterationEnd.getType()),
				iterationEnd.getKeyPositions(), "Unnamed Java Delta Iteration");
		iterationOperator.setMaximumNumberOfIterations(iterationEnd.getMaxIterations());
		
		DeltaIteration<D, W> iterationHead = iterationEnd.getIterationHead();

		DeltaIteration.SolutionSetPlaceHolder<D> solutionSetPlaceHolder = iterationHead.getSolutionSet();

		DeltaIteration.WorksetPlaceHolder<W> worksetPlaceHolder = iterationHead.getWorkset();

		translated.put(solutionSetPlaceHolder, iterationOperator.getSolutionSet());
		translated.put(worksetPlaceHolder, iterationOperator.getWorkset());

		Operator<D> translatedSolutionSet = translate(iterationEnd.getNextSolutionSet());
		Operator<W> translatedWorkset = translate(iterationEnd.getNextWorkset());
		
		iterationOperator.setNextWorkset(translatedWorkset);
		iterationOperator.setSolutionSetDelta(translatedSolutionSet);

		iterationOperator.setInitialSolutionSet(translate(iterationHead.getInitialSolutionSet()));
		iterationOperator.setInitialWorkset(translate(iterationHead.getInitialWorkset()));

		return iterationOperator;
	}
	
	private void translateBcVariables(DataSet<?> setOrOp, Operator<?> dataFlowOp) {
		// check if this is actually an operator that could have broadcast variables
		if (setOrOp instanceof UdfOperator) {
			if (!(dataFlowOp instanceof AbstractUdfOperator<?, ?>)) {
				throw new RuntimeException("Error while creating the data flow plan for the program: A UDF operation was not translated to a UDF operator.");
			}
			
			UdfOperator<?> udfOp = (UdfOperator<?>) setOrOp;
			AbstractUdfOperator<?, ?> udfDataFlowOp = (AbstractUdfOperator<?, ?>) dataFlowOp;
		
			for (Map.Entry<String, DataSet<?>> bcVariable : udfOp.getBroadcastSets().entrySet()) {
				Operator<?> bcInput = translate(bcVariable.getValue());
				udfDataFlowOp.setBroadcastVariable(bcVariable.getKey(), bcInput);
			}
		}
	}
}
