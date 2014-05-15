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
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.java.BulkIterationResultSet;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.DeltaIterationResultSet;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.operators.translation.JavaPlan;
import eu.stratosphere.api.java.operators.translation.PlanBulkIterationOperator;
import eu.stratosphere.api.java.operators.translation.PlanDeltaIterationOperator;
import eu.stratosphere.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class OperatorTranslation {
	
	/** The already translated operations */
	private Map<DataSet<?>, Operator> translated = new HashMap<DataSet<?>, Operator>();
	
	
	public JavaPlan translateToPlan(List<DataSink<?>> sinks, String jobName) {
		List<GenericDataSink> planSinks = new ArrayList<GenericDataSink>();
		
		for (DataSink<?> sink : sinks) {
			planSinks.add(translate(sink));
		}
		
		return new JavaPlan(planSinks); 
	}
	
	
	private GenericDataSink translate(DataSink<?> sink) {
		
		// translate the input recursively
		Operator input = translate(sink.getDataSet());
		
		// translate the sink itself and connect it to the input
		GenericDataSink translatedSink = sink.translateToDataFlow(input);
				
		return translatedSink;
	}
	
	
	private Operator translate(DataSet<?> dataSet) {
		// check if we have already translated that data set (operation or source)
		Operator previous = this.translated.get(dataSet);
		if (previous != null) {
			return previous;
		}
		
		Operator dataFlowOp;
		
		if (dataSet instanceof DataSource) {
			dataFlowOp = ((DataSource<?>) dataSet).translateToDataFlow();
		}
		else if (dataSet instanceof SingleInputOperator) {
			SingleInputOperator<?, ?, ?> op = (SingleInputOperator<?, ?, ?>) dataSet;
			
			// translate the input
			Operator input = translate(op.getInput());
			// translate the operation itself and connect it to the input
			eu.stratosphere.api.common.operators.SingleInputOperator<?> singleInDataFlowOp = op.translateToDataFlow(input);
			dataFlowOp = singleInDataFlowOp;
			
			if (dataSet instanceof UdfOperator<?> ) {
				SingleInputUdfOperator<?, ?, ?> udfOp = (SingleInputUdfOperator<?, ?, ?>) op;
				
				// set configuration parameters
				Configuration opParams = udfOp.getParameters();
				if (opParams != null) {
					singleInDataFlowOp.getParameters().addAll(opParams);
				}
				
				// set the semantic properties
				SingleInputSemanticProperties props = udfOp.getSematicProperties();
				if (props != null) {
					singleInDataFlowOp.setSemanticProperties(props);
				}
			}
		}
		else if (dataSet instanceof TwoInputOperator) {
			TwoInputOperator<?, ?, ?, ?> op = (TwoInputOperator<?, ?, ?, ?>) dataSet;
			
			// translate its inputs
			Operator input1 = translate(op.getInput1());
			Operator input2 = translate(op.getInput2());
			
			// translate the operation itself and connect it to the inputs
			eu.stratosphere.api.common.operators.DualInputOperator<?> binaryDataFlowOp = op.translateToDataFlow(input1, input2);
			dataFlowOp = binaryDataFlowOp;
			
			
			if (dataSet instanceof UdfOperator<?> ) {
				TwoInputUdfOperator<?, ?, ?, ?> udfOp = (TwoInputUdfOperator<?, ?, ?, ?>) op;
				
				// set configuration parameters
				Configuration opParams = udfOp.getParameters();
				if (opParams != null) {
					dataFlowOp.getParameters().addAll(opParams);
				}
				
				// set the semantic properties
				DualInputSemanticProperties props = udfOp.getSematicProperties();
				if (props != null) {
					binaryDataFlowOp.setSemanticProperties(props);
				}
			}
		}
		else if (dataSet instanceof BulkIterationResultSet<?>) {
			dataFlowOp = translateBulkIteration((BulkIterationResultSet<?>) dataSet);
		}
		else if (dataSet instanceof DeltaIterationResultSet<?, ?>) {
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
	
	private <T> BulkIteration translateBulkIteration(BulkIterationResultSet<T> iterationEnd) {
		PlanBulkIterationOperator<T> iterationOperator = new PlanBulkIterationOperator<T>("Bulk Iteration", iterationEnd.getType());
		IterativeDataSet<?> iterationHead = iterationEnd.getIterationHead();

		translated.put(iterationHead, iterationOperator.getPartialSolution());

		Operator translatedBody = translate(iterationEnd.getNextPartialSolution());
		iterationOperator.setNextPartialSolution(translatedBody);
		iterationOperator.setMaximumNumberOfIterations(iterationHead.getMaxIterations());
		iterationOperator.setInput(translate(iterationHead.getInput()));
		
		iterationOperator.getAggregators().addAll(iterationHead.getAggregators());
		
		if(iterationEnd.getTerminationCriterion() != null) {
			iterationOperator.setTerminationCriterion(translate(iterationEnd.getTerminationCriterion()));
		}

		return iterationOperator;
	}
	
	private <D, W> eu.stratosphere.api.common.operators.DeltaIteration translateDeltaIteration(DeltaIterationResultSet<D, W> iterationEnd) {
		PlanDeltaIterationOperator<D, W> iterationOperator = new PlanDeltaIterationOperator<D, W>(iterationEnd.getKeyPositions(), "Unnamed Java Delta Iteration", iterationEnd.getType(), iterationEnd.getWorksetType()); // always assume 0 as key position?
		iterationOperator.setMaximumNumberOfIterations(iterationEnd.getMaxIterations());
		
		DeltaIteration<D, W> iterationHead = iterationEnd.getIterationHead();

		DeltaIteration.SolutionSetPlaceHolder<D> solutionSetPlaceHolder = iterationHead.getSolutionSet();

		DeltaIteration.WorksetPlaceHolder<W> worksetPlaceHolder = iterationHead.getWorkset();

		translated.put(solutionSetPlaceHolder, iterationOperator.getSolutionSet());
		translated.put(worksetPlaceHolder, iterationOperator.getWorkset());

		Operator translatedSolutionSet = translate(iterationEnd.getNextSolutionSet());
		Operator translatedWorkset = translate(iterationEnd.getNextWorkset());
		
		iterationOperator.setNextWorkset(translatedWorkset);
		iterationOperator.setSolutionSetDelta(translatedSolutionSet);

		iterationOperator.setInitialSolutionSet(translate(iterationHead.getInitialSolutionSet()));
		iterationOperator.setInitialWorkset(translate(iterationHead.getInitialWorkset()));

		return iterationOperator;
	}
	
	private void translateBcVariables(DataSet<?> setOrOp, Operator dataFlowOp) {
		// check if this is actually an operator that could have broadcast variables
		if (setOrOp instanceof UdfOperator) {
			if (!(dataFlowOp instanceof AbstractUdfOperator<?>)) {
				throw new RuntimeException("Error while creating the data flow plan for the program: A UDF operation was not translated to a UDF operator.");
			}
			
			UdfOperator<?> udfOp = (UdfOperator<?>) setOrOp;
			AbstractUdfOperator<?> udfDataFlowOp = (AbstractUdfOperator<?>) dataFlowOp;
		
			for (Map.Entry<String, DataSet<?>> bcVariable : udfOp.getBroadcastSets().entrySet()) {
				Operator bcInput = translate(bcVariable.getValue());
				udfDataFlowOp.setBroadcastVariable(bcVariable.getKey(), bcInput);
			}
		}
	}
}
