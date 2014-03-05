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
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 */
public class PlanGroupReduceOperator<IN, OUT> extends GroupReduceOperatorBase<GenericGroupReduce<IN,OUT>>
	implements UnaryJavaPlanNode<IN, OUT>
{

	private final TypeInformation<IN> inType;
	
	private final TypeInformation<OUT> outType;
	
	
	public PlanGroupReduceOperator(GroupReduceFunction<IN, OUT> udf, int[] logicalGroupingFields, String name, 
				TypeInformation<IN> inputType, TypeInformation<OUT> outputType)
	{
		super(udf, logicalGroupingFields, name);
		
		this.inType = inputType;
		this.outType = outputType;
	}
	
	
	@Override
	public TypeInformation<OUT> getReturnType() {
		return this.outType;
	}

	@Override
	public TypeInformation<IN> getInputType() {
		return this.inType;
	}
}
