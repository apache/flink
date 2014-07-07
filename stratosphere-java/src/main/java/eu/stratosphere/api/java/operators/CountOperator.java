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

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;

/**
 * A {@link DataSet} that is the result of a count transformation.
 * <p/>
 * The count will be executed as a map-reduce. The map operator maps every element of the input to a 1 and the all
 * reduce sums the ones up to the total count.
 *
 * @param <IN> The type of the data set aggregated by the operator.
 */
public class CountOperator<IN> extends SingleInputUdfOperator<IN, Long, CountOperator<IN>> {

	public CountOperator(DataSet<IN> input) {
		super(input, BasicTypeInfo.LONG_TYPE_INFO);
	}

	@Override
	protected eu.stratosphere.api.common.operators.SingleInputOperator<Long, Long, ReduceFunction<Long>> translateToDataFlow(Operator<IN> input) {
		// map to ones
		UnaryOperatorInformation<IN, Long> countMapOpInfo =
				new UnaryOperatorInformation<IN, Long>(getInputType(), BasicTypeInfo.LONG_TYPE_INFO);
		MapOperatorBase<IN, Long, MapFunction<IN, Long>> countMapOp =
				new MapOperatorBase<IN, Long, MapFunction<IN, Long>>(
						new CountingMapUdf(), countMapOpInfo, "Count: map to ones");

		countMapOp.setInput(input);
		countMapOp.setDegreeOfParallelism(input.getDegreeOfParallelism());

		// sum ones
		UnaryOperatorInformation<Long, Long> countReduceOpInfo =
				new UnaryOperatorInformation<Long, Long>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
		ReduceOperatorBase<Long, ReduceFunction<Long>> countReduceOp =
				new ReduceOperatorBase<Long, ReduceFunction<Long>>(
						new CountingReduceUdf(), countReduceOpInfo, "Count: sum ones");

		countReduceOp.setInput(countMapOp);
		countReduceOp.setDegreeOfParallelism(1);
		countReduceOp.setInitialValue(countReduceOpInfo.getInputType().createSerializer(), 0L);

		return countReduceOp;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public static class CountingMapUdf<IN> extends MapFunction<IN, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long map(IN value) throws Exception {
			return 1L;
		}
	}

	public static class CountingReduceUdf extends ReduceFunction<Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}
	}
}
