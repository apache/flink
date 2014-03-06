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

import eu.stratosphere.api.common.functions.GenericCrosser;
import eu.stratosphere.api.common.operators.base.CrossOperatorBase;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Reference;

public class PlanCrossOperator<IN1, IN2, OUT> 
	extends CrossOperatorBase<GenericCrosser<Reference<IN1>, Reference<IN2>, Reference<OUT>>> 
	implements BinaryJavaPlanNode<IN1, IN2, OUT>{
	
	private final TypeInformation<IN1> inType1;
	private final TypeInformation<IN2> inType2;
	private final TypeInformation<OUT> outType;
	

	public PlanCrossOperator(
			CrossFunction<IN1, IN2, OUT> udf,
			String name,
			 TypeInformation<IN1> inType1, TypeInformation<IN2> inType2, TypeInformation<OUT> outType) {
		super(new ReferenceWrappingCrosser<IN1, IN2, OUT>(udf), name);
		
		this.inType1 = inType1;
		this.inType2 = inType2;
		this.outType = outType;
		
	}
	
	
	public static final class ReferenceWrappingCrosser<IN1, IN2, OUT> 
		extends WrappingFunction<CrossFunction<IN1, IN2, OUT>>
		implements GenericCrosser<Reference<IN1>, Reference<IN2>, Reference<OUT>>
	{
		
		private static final long serialVersionUID = 1L;
		
		private final Reference<OUT> ref = new Reference<OUT>();
	
		protected ReferenceWrappingCrosser(
				CrossFunction<IN1, IN2, OUT> wrappedFunction) {
			super(wrappedFunction);
		}


		@Override
		public void cross(Reference<IN1> record1, Reference<IN2> record2,
				Collector<Reference<OUT>> out) throws Exception {
			this.ref.ref = this.wrappedFunction.cross(record1.ref, record2.ref);

			out.collect(ref);
		}
	}


	@Override
	public TypeInformation<OUT> getReturnType() {
		return this.outType;
	}


	@Override
	public TypeInformation<IN1> getInputType1() {
		return this.inType1;
	}


	@Override
	public TypeInformation<IN2> getInputType2() {
		return this.inType2;
	}

}
