/**
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;

public class CollectionExecutor {
	
	private final Map<Operator<?>, List<?>> intermediateResults;
	
	private final Map<String, Accumulator<?, ?>> accumulators;
	
	
	public CollectionExecutor() {
		this.intermediateResults = new HashMap<Operator<?>, List<?>>();
		this.accumulators = new HashMap<String, Accumulator<?,?>>();
	}
	

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
	
//	private void execute(Operator<?>... operators) throws Exception {
//		for (Operator<?> op : operators) {
//			execute(op);
//		}
//	}
	
	
	private List<?> execute(Operator<?> operator) throws Exception {
		List<?> result = this.intermediateResults.get(operator);
		
		// if it has already been computed, use the cached variant
		if (result != null) {
			return result;
		}
		
		if (operator instanceof SingleInputOperator) {
			return executeUnaryOperator((SingleInputOperator<?, ?, ?>) operator);
		}
		else if (operator instanceof DualInputOperator) {
			return executeBinaryOperator((DualInputOperator<?, ?, ?, ?>) operator);
		}
		else if (operator instanceof GenericDataSourceBase) {
			return executeDataSource((GenericDataSourceBase<?, ?>) operator);
		}
		else if (operator instanceof GenericDataSinkBase) {
			executeDataSink((GenericDataSinkBase<?>) operator);
			result = Collections.emptyList();
		}
		
		this.intermediateResults.put(operator, result);
		
		return result;
	}
	
	
	
	private <IN> void executeDataSink(GenericDataSinkBase<?> sink) throws Exception {
		Operator<?> inputOp = sink.getInput();
		if (inputOp == null) {
			throw new InvalidProgramException("The data sink " + sink.getName() + " has no input.");
		}
		
		@SuppressWarnings("unchecked")
		List<IN> input = (List<IN>) execute(inputOp);
		
		@SuppressWarnings("unchecked")
		GenericDataSinkBase<IN> typedSink = (GenericDataSinkBase<IN>) sink;

		typedSink.executeOnCollections(input);
	}
	
	private <OUT> List<OUT> executeDataSource(GenericDataSourceBase<?, ?> source) throws Exception {
		@SuppressWarnings("unchecked")
		GenericDataSourceBase<OUT, ?> typedSource = (GenericDataSourceBase<OUT, ?>) source;
		return typedSource.executeOnCollections();
	}
	
	private <IN, OUT> List<OUT> executeUnaryOperator(SingleInputOperator<?, ?, ?> operator) throws Exception {
		Operator<?> inputOp = operator.getInput();
		if (inputOp == null) {
			throw new InvalidProgramException("The unary operation " + operator.getName() + " has no input.");
		}
		
		@SuppressWarnings("unchecked")
		List<IN> inputData = (List<IN>) execute(inputOp);
		
		@SuppressWarnings("unchecked")
		SingleInputOperator<IN, OUT, ?> typedOp = (SingleInputOperator<IN, OUT, ?>) operator;
		
		// build the runtime context and compute broadcast variables, if necessary
		RuntimeUDFContext ctx;
		if (RichFunction.class.isAssignableFrom(typedOp.getUserCodeWrapper().getUserCodeClass())) {
			ctx = new RuntimeUDFContext(operator.getName(), 1, 0);
			
			for (Map.Entry<String, Operator<?>> bcInputs : operator.getBroadcastInputs().entrySet()) {
				List<?> bcData = execute(bcInputs.getValue());
				ctx.setBroadcastVariable(bcInputs.getKey(), bcData);
			}
		} else {
			ctx = null;
		}
		
		List<OUT> result = typedOp.executeOnCollections(inputData, ctx);
		
		if (ctx != null) {
			AccumulatorHelper.mergeInto(this.accumulators, ctx.getAllAccumulators());
		}
		return result;
	}
	
	private <IN1, IN2, OUT> List<OUT> executeBinaryOperator(DualInputOperator<?, ?, ?, ?> operator) throws Exception {
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
		List<IN1> inputData1 = (List<IN1>) execute(inputOp1);
		@SuppressWarnings("unchecked")
		List<IN2> inputData2 = (List<IN2>) execute(inputOp2);
		
		@SuppressWarnings("unchecked")
		DualInputOperator<IN1, IN2, OUT, ?> typedOp = (DualInputOperator<IN1, IN2, OUT, ?>) operator;
		
		// build the runtime context and compute broadcast variables, if necessary
		RuntimeUDFContext ctx;
		if (RichFunction.class.isAssignableFrom(typedOp.getUserCodeWrapper().getUserCodeClass())) {
			ctx = new RuntimeUDFContext(operator.getName(), 1, 0);
			
			for (Map.Entry<String, Operator<?>> bcInputs : operator.getBroadcastInputs().entrySet()) {
				List<?> bcData = execute(bcInputs.getValue());
				ctx.setBroadcastVariable(bcInputs.getKey(), bcData);
			}
		} else {
			ctx = null;
		}
		
		List<OUT> result = typedOp.executeOnCollections(inputData1, inputData2, ctx);
		
		if (ctx != null) {
			AccumulatorHelper.mergeInto(this.accumulators, ctx.getAllAccumulators());
		}
		return result;
	}
}
