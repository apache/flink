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

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.base.PlainMapOperatorBase;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class PlanProjectOperator<T, R extends Tuple> 
	extends PlainMapOperatorBase<GenericMap<T, R>>
	implements UnaryJavaPlanNode<T, R>
{
	private final TypeInformation<T> inType;
	private final TypeInformation<R> outType;
	
	
	public PlanProjectOperator(int[] fields, String name, TypeInformation<T> inType, TypeInformation<R> outType) {
		super(new MapProjector<T, R>(fields, outType.createSerializer()), name);
		this.inType = inType;
		this.outType = outType;
	}
	
	@Override
	public TypeInformation<R> getReturnType() {
		return this.outType;
	}

	@Override
	public TypeInformation<T> getInputType() {
		return this.inType;
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	public static final class MapProjector<T, R extends Tuple> 
		extends AbstractFunction
		implements GenericMap<T, R>
	{
		private static final long serialVersionUID = 1L;
		
		private final int[] fields;
		private final R outTuple;
		
		private MapProjector(int[] fields, TypeSerializer<R> serializer) {
			this.fields = fields;
			this.outTuple = serializer.createInstance();
		}

		// TODO We should use code generation for this.
		@Override
		public R map(T inTuple) throws Exception {
			
			for(int i=0; i<fields.length; i++) {
				outTuple.setField(((Tuple)inTuple).getField(fields[i]), i);
			}
			return outTuple;
		}
	}
}
