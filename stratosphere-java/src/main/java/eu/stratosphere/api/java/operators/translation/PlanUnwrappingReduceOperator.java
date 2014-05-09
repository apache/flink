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

import eu.stratosphere.api.common.functions.GenericReduce;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TypeInformation;


public class PlanUnwrappingReduceOperator<T, K> extends ReduceOperatorBase<GenericReduce<Tuple2<K, T>>>
	implements UnaryJavaPlanNode<Tuple2<K, T>, Tuple2<K, T>>
{
	
	private final TypeInformation<Tuple2<K, T>> typeInfoWithKey;


	public PlanUnwrappingReduceOperator(ReduceFunction<T> udf, Keys.SelectorFunctionKeys<T, K> key, String name,
			TypeInformation<T> type, TypeInformation<Tuple2<K, T>> typeInfoWithKey)
	{
		super(new ReduceWrapper<T, K>(udf), key.computeLogicalKeyPositions(), name);
		
		this.typeInfoWithKey = typeInfoWithKey;
	}
	
	
	@Override
	public TypeInformation<Tuple2<K, T>> getReturnType() {
		return this.typeInfoWithKey;
	}

	@Override
	public TypeInformation<Tuple2<K, T>> getInputType() {
		return this.typeInfoWithKey;
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ReduceWrapper<T, K> extends WrappingFunction<ReduceFunction<T>>
		implements GenericReduce<Tuple2<K, T>>
	{
		private static final long serialVersionUID = 1L;
		

		private ReduceWrapper(ReduceFunction<T> wrapped) {
			super(wrapped);
		}

		@Override
		public Tuple2<K, T> reduce(Tuple2<K, T> value1, Tuple2<K, T> value2) throws Exception {
			value1.f1 = this.wrappedFunction.reduce(value1.f1, value2.f1);
			return value1;
		}
	}
}
