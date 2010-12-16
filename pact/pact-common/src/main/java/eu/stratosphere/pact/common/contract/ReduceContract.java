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
 * @TODO
 * @author DIMA
 */
public class ReduceContract<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		SingleInputContract<IK, IV, OK, OV> {
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Combinable {
	};

	private static String defaultName = "Reduce #";

	private static int nextID = 1;

	public ReduceContract(Class<? extends ReduceStub<IK, IV, OK, OV>> reducer, String n) {
		super(reducer, n);
	}

	public ReduceContract(Class<? extends ReduceStub<IK, IV, OK, OV>> reducer) {
		super(reducer, defaultName + (nextID++));
	}

	public boolean isCombinable() {
		return (super.clazz.getAnnotation(Combinable.class) != null);
	}

}
