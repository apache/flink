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

package eu.stratosphere.pact.generic.contract;

import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.generic.stub.GenericMapper;


/**
 * MapContract represents a Pact with a Map Input Contract.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Map works on a single input and calls the first-order user function of a {@see eu.stratosphere.pact.common.stub.MapStub} 
 * for each record independently.
 * 
 * @see MapStub
 * 
 * @author Aljoscha Krettek
 */
public class GenericMapContract<T extends GenericMapper<?, ?>> extends SingleInputContract<T>
{
	public GenericMapContract(Class<? extends T> udf, String name) {
		super(udf, name);
	}
}
