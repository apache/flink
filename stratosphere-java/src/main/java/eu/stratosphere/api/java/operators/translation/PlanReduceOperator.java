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

import java.util.Iterator;

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;

/**
 *
 */
public class PlanReduceOperator<T> extends GroupReduceOperatorBase<GenericGroupReduce<T,T>>
	implements UnaryJavaPlanNode<T, T>
{

	private final TypeInformation<T> type;
	
	
	public PlanReduceOperator(ReduceFunction<T> udf, int[] logicalGroupingFields, String name, TypeInformation<T> type) {
		super(new ReferenceWrappingReducer<T>(udf), logicalGroupingFields, name);
		this.type = type;
	}
	
	
	@Override
	public TypeInformation<T> getReturnType() {
		return this.type;
	}

	@Override
	public TypeInformation<T> getInputType() {
		return this.type;
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ReferenceWrappingReducer<T> extends WrappingFunction<ReduceFunction<T>>
		implements GenericGroupReduce<T, T>
	{

		private static final long serialVersionUID = 1L;
		
		private ReferenceWrappingReducer(ReduceFunction<T> wrapped) {
			super(wrapped);
		}


		@Override
		public void reduce(Iterator<T> values, Collector<T> out) throws Exception {
			T curr = values.next();
			
			while (values.hasNext()) {
				curr = this.wrappedFunction.reduce(curr, values.next());
			}
			
			out.collect(curr);
		}

		@Override
		public void combine(Iterator<T> values, Collector<T> out) throws Exception {
			reduce(values, out);
		}

	}
}
