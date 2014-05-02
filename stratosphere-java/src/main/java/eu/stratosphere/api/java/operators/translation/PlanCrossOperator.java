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

public class PlanCrossOperator<IN1, IN2, OUT> 
	extends CrossOperatorBase<GenericCrosser<IN1, IN2, OUT>>
	implements BinaryJavaPlanNode<IN1, IN2, OUT>{
	
	private final TypeInformation<IN1> inType1;
	private final TypeInformation<IN2> inType2;
	private final TypeInformation<OUT> outType;
	

	public PlanCrossOperator(
			CrossFunction<IN1, IN2, OUT> udf,
			String name,
			TypeInformation<IN1> inType1, TypeInformation<IN2> inType2, TypeInformation<OUT> outType) {
		super(udf, name);
		
		this.inType1 = inType1;
		this.inType2 = inType2;
		this.outType = outType;
		
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
