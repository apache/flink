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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.api.common.operators.AbstractUdfOperator;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIterativeDataSet;
import eu.stratosphere.api.java.DeltaIterativeResultDataSet;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.IterativeResultDataSet;
import eu.stratosphere.api.java.operators.translation.JavaPlan;
import eu.stratosphere.api.java.operators.translation.PlanBulkIterationOperator;
import eu.stratosphere.api.java.operators.translation.PlanDeltaIterationOperator;


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
			dataFlowOp = op.translateToDataFlow(input);
		}
		else if (dataSet instanceof TwoInputOperator) {
			TwoInputOperator<?, ?, ?, ?> op = (TwoInputOperator<?, ?, ?, ?>) dataSet;
			
			// translate its inputs
			Operator input1 = translate(op.getInput1());
			Operator input2 = translate(op.getInput2());
			
			// translate the operation itself and connect it to the inputs
			dataFlowOp = op.translateToDataFlow(input1, input2);
		}
		else if (dataSet instanceof IterativeResultDataSet<?>) {
			dataFlowOp = translateBulkIteration((IterativeResultDataSet<?>) dataSet);
		}
		else if (dataSet instanceof DeltaIterativeResultDataSet<?, ?>) {
			dataFlowOp = translateDeltaIteration((DeltaIterativeResultDataSet<?, ?>) dataSet);
		}
		else {
			throw new RuntimeException("Error while creating the data flow plan for the program: Unknown operator or data set type: " + dataSet);
		}
		
		this.translated.put(dataSet, dataFlowOp);
		
		// take care of broadcast variables
		translateBcVariables(dataSet, dataFlowOp);
		
		return dataFlowOp;
	}
	
	private BulkIteration translateBulkIteration(IterativeResultDataSet<?> iterationEnd) {
		PlanBulkIterationOperator iterationOperator = new PlanBulkIterationOperator("Bulk Iteration", iterationEnd.getType());
		IterativeDataSet<?> iterationHead = iterationEnd.getIterationHead();

		translated.put(iterationHead, iterationOperator.getPartialSolution());

		Operator translatedBody = translate(iterationEnd.getNextPartialSolution());
		iterationOperator.setNextPartialSolution(translatedBody);
		iterationOperator.setMaximumNumberOfIterations(iterationHead.getMaxIterations());
		iterationOperator.setInput(translate(iterationHead.getInput()));
		
		iterationOperator.getAggregators().addAll(iterationHead.getAggregators());

		return iterationOperator;
	}
	
	private DeltaIteration translateDeltaIteration(DeltaIterativeResultDataSet<?, ?> iterationEnd) {
		PlanDeltaIterationOperator iterationOperator = new PlanDeltaIterationOperator(iterationEnd.getKeyPositions(), "Unnamed Java Delta Iteration", iterationEnd.getType(), iterationEnd.getWorksetType()); // always assume 0 as key position?
		iterationOperator.setMaximumNumberOfIterations(iterationEnd.getMaxIterations());
		
		DeltaIterativeDataSet<?, ?> iterationHead = iterationEnd.getIterationHead();

		translated.put(iterationEnd.getIterationHeadSolutionSet(), iterationOperator.getSolutionSet());
		translated.put(iterationHead, iterationOperator.getWorkset());

		Operator translatedSolutionSet = translate(iterationEnd.getNextSolutionSet());
		Operator translatedWorkset = translate(iterationEnd.getNextWorkset());
		
		iterationOperator.setNextWorkset(translatedWorkset);
		iterationOperator.setSolutionSetDelta(translatedSolutionSet);

		iterationOperator.setInitialSolutionSet(translate(iterationHead.getInput1()));
		iterationOperator.setInitialWorkset(translate(iterationHead.getInput2()));

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
