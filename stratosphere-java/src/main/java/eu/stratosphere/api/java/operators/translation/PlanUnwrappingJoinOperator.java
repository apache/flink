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

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;

public class PlanUnwrappingJoinOperator<I1, I2, OUT, K> 
	extends JoinOperatorBase<GenericJoiner<Tuple2<K, I1>, Tuple2<K, I2>, OUT>>
	implements BinaryJavaPlanNode<Tuple2<K, I1>, Tuple2<K, I2>, OUT>
{

	private final TypeInformation<Tuple2<K, I1>> inTypeWithKey1;
	
	private final TypeInformation<Tuple2<K, I2>> inTypeWithKey2;
	
	private final TypeInformation<OUT> outType;

	public PlanUnwrappingJoinOperator(JoinFunction<I1, I2, OUT> udf, 
			Keys.SelectorFunctionKeys<I1, K> key1, Keys.SelectorFunctionKeys<I2, K> key2, String name,
			TypeInformation<OUT> type, TypeInformation<Tuple2<K, I1>> typeInfoWithKey1, TypeInformation<Tuple2<K, I2>> typeInfoWithKey2)
	{
		super(new TupleUnwrappingJoiner<I1, I2, OUT, K>(udf), key1.computeLogicalKeyPositions(), key2.computeLogicalKeyPositions(), name);
		this.outType = type;
		
		this.inTypeWithKey1 = typeInfoWithKey1;
		this.inTypeWithKey2 = typeInfoWithKey2;
	}
	

	@Override
	public TypeInformation<OUT> getReturnType() {
		return outType;
	}

	@Override
	public TypeInformation<Tuple2<K, I1>> getInputType1() {
		return inTypeWithKey1;
	}


	@Override
	public TypeInformation<Tuple2<K, I2>> getInputType2() {
		return inTypeWithKey2;
	}


	
	// --------------------------------------------------------------------------------------------
	
	public static final class TupleUnwrappingJoiner<I1, I2, OUT, K>
		extends WrappingFunction<JoinFunction<I1, I2, OUT>>
		implements GenericJoiner<Tuple2<K, I1>, Tuple2<K, I2>, OUT>
	{

		private static final long serialVersionUID = 1L;
		
		private TupleUnwrappingJoiner(JoinFunction<I1, I2, OUT> wrapped) {
			super(wrapped);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void join(Tuple2<K, I1> value1, Tuple2<K, I2> value2,
				Collector<OUT> out) throws Exception {
			out.collect(wrappedFunction.join((I1)(value1.getField(1)), (I2)(value2.getField(1))));
		}
		
	}

}
