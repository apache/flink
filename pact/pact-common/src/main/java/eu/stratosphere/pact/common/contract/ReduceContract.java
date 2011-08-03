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

package eu.stratosphere.pact.common.contract;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * ReduceContract represents a Reduce InputContract of the PACT Programming Model.
 * InputContracts are second-order functions. 
 * They have one or multiple input sets of key/value-pairs and a first-order user function (stub implementation).
 * <p> 
 * Reduce works on a single input and calls the first-order user function of a 
 * {@see eu.stratosphere.pact.common.stub.ReduceStub} for each group of key/value-pairs that share the same key independently.
 * 
 * @see eu.stratosphere.pact.common.stub.ReduceStub
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class ReduceContract<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		SingleInputContract<IK, IV, OK, OV> {
	
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Combinable {
	};

	private static String defaultName = "Reduce #";

	private static int nextID = 1;

	/**
	 * Creates a ReduceContract with the provided {@see eu.stratosphere.pact.common.stub.ReduceStub} implementation 
	 * and the given name. 
	 * 
	 * @param reducer The {@link ReduceStub} implementation for this Reduce InputContract.
	 * @param n The name of the PACT.
	 */
	public ReduceContract(Class<? extends ReduceStub<IK, IV, OK, OV>> reducer, String n) {
		super(reducer, n);
	}

	/**
	 * Creates a ReduceContract with the provided {@see eu.stratosphere.pact.common.stub.ReduceStub} implementation
	 * and a default name.
	 * 
	 * @param reducer The {@link ReduceStub} implementation for this Reduce InputContract.
	 */
	public ReduceContract(Class<? extends ReduceStub<IK, IV, OK, OV>> reducer) {
		super(reducer, defaultName + (nextID++));
	}

	/**
	 * Returns true if the ReduceContract is annotated with a Combinable annotation.
	 * The annotation indicates that the contract's {@see eu.stratosphere.pact.common.stub.ReduceStub} implements 
	 * the {@see  eu.stratosphere.pact.common.stub.ReduceStub#combine(Key, java.util.Iterator, eu.stratosphere.pact.common.stub.Collector) method.
	 * 
	 * @return true if the ReduceContract is combinable, false otherwise.
	 */
	public boolean isCombinable() {
		return (getUserCodeClass().getAnnotation(Combinable.class) != null);
	}

}
