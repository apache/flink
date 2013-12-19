/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.arraymodel.operators;

import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.arraymodel.functions.ReduceWithKeyFunction;

/**
 * MapOperator represents a Pact with a Map Input Contract.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each record independently.
 * 
 * @see ReduceFunction
 */
public class ReduceWithKeyOperator extends ReduceOperatorBase<ReduceWithKeyFunction>
{
	public ReduceWithKeyOperator(Class<? extends ReduceWithKeyFunction> udf, int keyPosition, String name) {
		super(new UserCodeClassWrapper<ReduceWithKeyFunction>(udf), new int[] {keyPosition}, name);
		getParameters().setInteger(ReduceWithKeyFunction.KEY_INDEX_PARAM_KEY, keyPosition);
	}
	
	public ReduceWithKeyOperator(ReduceWithKeyFunction udf, int keyPosition, String name) {
		super(new UserCodeObjectWrapper<ReduceWithKeyFunction>(udf), new int[] {keyPosition}, name);
		getParameters().setInteger(ReduceWithKeyFunction.KEY_INDEX_PARAM_KEY, keyPosition);
	}
}
