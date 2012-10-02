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

package eu.stratosphere.pact.common.generic.contract;

import eu.stratosphere.pact.common.generic.GenericMatcher;
import eu.stratosphere.pact.common.generic.contract.DualInputContract;
import eu.stratosphere.pact.common.stubs.MatchStub;

/**
 * CrossContract represents a Match InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Match works on two inputs and calls the first-order function of a {@link MatchStub} 
 * for each combination of record from both inputs that share the same key independently. In that sense, it is very
 * similar to an inner join.
 * 
 * @see MatchStub
 */
public class GenericMatchContract<T extends GenericMatcher<?, ?, ?>> extends DualInputContract<T>
{
	public GenericMatchContract(Class <? extends T> udf, int[] keyPositions1, int[] keyPositions2, String name) {
		super(udf, keyPositions1, keyPositions2, name);
	}
}
