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
package eu.stratosphere.api.java.operators.translation;

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;

import java.util.Iterator;

/**
 *
 */
public class PlanUnwrappingReduceOperator<T, K> extends GroupReduceOperatorBase<GenericGroupReduce<Tuple2<K, T>, T>>
	implements UnaryJavaPlanNode<Tuple2<K, T>, T>
{

	private final TypeInformation<T> type;
	
	private final TypeInformation<Tuple2<K, T>> typeInfoWithKey;


	public PlanUnwrappingReduceOperator(ReduceFunction<T> udf, Keys.SelectorFunctionKeys<T, K> key, String name,
			TypeInformation<T> type, TypeInformation<Tuple2<K, T>> typeInfoWithKey)
	{
		super(new ReduceGroupWrapper<T, K>(udf), key.computeLogicalKeyPositions(), name);
		this.type = type;
		
		this.typeInfoWithKey = typeInfoWithKey;
	}
	
	
	@Override
	public TypeInformation<T> getReturnType() {
		return this.type;
	}

	@Override
	public TypeInformation<Tuple2<K, T>> getInputType() {
		return this.typeInfoWithKey;
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ReduceGroupWrapper<T, K> extends WrappingFunction<ReduceFunction<T>>
		implements GenericGroupReduce<Tuple2<K, T>, T>
	{

		private static final long serialVersionUID = 1L;
		

		private ReduceGroupWrapper(ReduceFunction<T> wrapped) {
			super(wrapped);
		}


		@Override
		public void reduce(Iterator<Tuple2<K, T>> values, Collector<T> out) throws Exception {
			T curr = values.next().T2();
			
			while (values.hasNext()) {
				curr = this.wrappedFunction.reduce(curr, values.next().T2());
			}
			
			out.collect(curr);
		}

		@Override
		public void combine(Iterator<Tuple2<K, T>> values, Collector<Tuple2<K, T>> out) throws Exception {
			
			Tuple2<K, T> currentTuple = values.next();
			
			T curr = currentTuple.T2();

			while (values.hasNext()) {
				currentTuple = values.next();
				curr = this.wrappedFunction.reduce(curr, currentTuple.T2());
			}

			out.collect(new Tuple2<K, T>(currentTuple.T1(), curr));
		}

	}
}
