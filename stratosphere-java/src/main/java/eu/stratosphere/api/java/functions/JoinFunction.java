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
package eu.stratosphere.api.java.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.util.Collector;


/**
 * Base class for a user defined join function. 
 *
 * (Use a {@link CoGroupFunction} to perform an outer join)
 * 
 * @param <IN1> Type of the incoming objects from the first input
 * @param <IN2> Type of the objects from the second input
 * @param <OUT> Type of the resulting objects.
 */
public abstract class JoinFunction<IN1, IN2, OUT> extends AbstractFunction implements GenericJoiner<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * A user-defined method for performing transformations after a join.
	 * The method with the user-code is called with an object from each input.
	 * The two objects match on the given join criterion.
	 * Users can use this method to combine the data from the two inputs into a new object.
	 * 
	 * @param first object from first input
	 * @param second object from second input
	 * @return resulting object.
	 * @throws Exception
	 */
	public abstract OUT join(IN1 first, IN2 second) throws Exception;
	
	
	
	@Override
	public final void join(IN1 value1, IN2 value2, Collector<OUT> out) throws Exception {
		out.collect(join(value1, value2));
	}
}
