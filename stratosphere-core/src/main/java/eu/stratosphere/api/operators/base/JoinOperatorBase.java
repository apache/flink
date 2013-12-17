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

package eu.stratosphere.api.operators.base;

import eu.stratosphere.api.functions.GenericJoiner;
import eu.stratosphere.api.operators.DualInputOperator;
import eu.stratosphere.api.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.operators.util.UserCodeWrapper;

/**
 * MatchContract represents a Match InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. They have one or multiple input sets of records and a first-order
 * user function (stub implementation).
 * <p> 
 * Match works on two inputs and calls the first-order function of a {@link GenericJoiner} 
 * for each combination of record from both inputs that share the same key independently. In that sense, it is very
 * similar to an inner join.
 * 
 * @see GenericJoiner
 */
public class JoinOperatorBase<T extends GenericJoiner<?, ?, ?>> extends DualInputOperator<T>
{
	public JoinOperatorBase(UserCodeWrapper<T> udf, int[] keyPositions1, int[] keyPositions2, String name) {
		super(udf, keyPositions1, keyPositions2, name);
	}
	
	public JoinOperatorBase(T udf, int[] keyPositions1, int[] keyPositions2, String name) {
		super(new UserCodeObjectWrapper<T>(udf), keyPositions1, keyPositions2, name);
	}
	
	public JoinOperatorBase(Class<? extends T> udf, int[] keyPositions1, int[] keyPositions2, String name) {
		super(new UserCodeClassWrapper<T>(udf), keyPositions1, keyPositions2, name);
	}
}
