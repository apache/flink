/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.array.contract;

import eu.stratosphere.pact.array.stubs.ReduceWithKeyStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.generic.contract.GenericReduceContract;

/**
 * MapContract represents a Pact with a Map Input Contract.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each record independently.
 * 
 * @see ReduceStub
 */
public class ReduceWithKeyContract extends GenericReduceContract<ReduceWithKeyStub>
{
	public ReduceWithKeyContract(Class <? extends ReduceWithKeyStub> udf, int keyPosition, String name) {
		super(udf, new int[] {keyPosition}, name);
		getParameters().setInteger(ReduceWithKeyStub.KEY_INDEX_PARAM_KEY, keyPosition);
	}
}
