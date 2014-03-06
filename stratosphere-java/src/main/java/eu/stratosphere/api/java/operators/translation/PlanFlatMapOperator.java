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

import eu.stratosphere.api.common.functions.GenericFlatMap;
import eu.stratosphere.api.common.operators.base.FlatMapOperatorBase;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Reference;

/**
 *
 */
public class PlanFlatMapOperator<T, O> extends FlatMapOperatorBase<GenericFlatMap<Reference<T>, Reference<O>>>
	implements UnaryJavaPlanNode<T, O>
{
	private final TypeInformation<T> inType;
	
	private final TypeInformation<O> outType;
	
	
	public PlanFlatMapOperator(FlatMapFunction<T, O> udf, String name, TypeInformation<T> inType, TypeInformation<O> outType) {
		super(new ReferenceWrappingFlatMapper<T, O>(udf), name);
		this.inType = inType;
		this.outType = outType;
	}
	
	@Override
	public TypeInformation<O> getReturnType() {
		return this.outType;
	}

	@Override
	public TypeInformation<T> getInputType() {
		return this.inType;
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ReferenceWrappingFlatMapper<IN, OUT> extends WrappingFunction<FlatMapFunction<IN, OUT>>
		implements GenericFlatMap<Reference<IN>, Reference<OUT>>
	{

		private static final long serialVersionUID = 1L;
		
		private final ReferenceWrappingCollector<OUT> coll = new ReferenceWrappingCollector<OUT>();
		
		private ReferenceWrappingFlatMapper(FlatMapFunction<IN, OUT> wrapped) {
			super(wrapped);
		}

		@Override
		public final void flatMap(Reference<IN> value, Collector<Reference<OUT>> out) throws Exception {
			coll.set(out);
			this.wrappedFunction.flatMap(value.ref, coll);
		}
	}
}
