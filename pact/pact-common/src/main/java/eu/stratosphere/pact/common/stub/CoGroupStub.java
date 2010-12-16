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

package eu.stratosphere.pact.common.stub;

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType3;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType4;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType5;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * The CoGroupStub must be extended to provide a cogrouper implementation which
 * is called by a CoGroup PACT. By definition, a CoGroup PACT has two input sets
 * of key-value pairs. It calls the cogrouper implementation once for each
 * distinct key of its inputs. Together with the key, two iterators over the
 * values of both inputs that were paired with that key are handed to the <code>coGroup()</code> method. For details on
 * the CoGroup PACT read the
 * documentation of the PACT programming model.
 * <p>
 * The CoGroupStub extension must be parametrized with the types of the keys and values of its both inputs and output.
 * <p>
 * For a coGroup implementation, the <code>coGroup()</code> method must be implemented.
 * 
 * @author Fabian Hueske
 * @param <IK>
 *        Type of the input key.
 * @param <IV1>
 *        Type of the first input's value.
 * @param <IV2>
 *        Type of the second input's value.
 * @param <OK>
 *        Type of the output key.
 * @param <OV>
 *        Type of the output value.
 */
public abstract class CoGroupStub<IK extends Key, IV1 extends Value, IV2 extends Value, OK extends Key, OV extends Value>
		extends DualInputStub<IK, IV1, IK, IV2, OK, OV> {

	/**
	 * This method must be implemented to provide a user implementation of a
	 * matcher. It is called for each two key-value pairs that share the same
	 * key and come from different inputs.
	 * 
	 * @param key
	 *        The key that is paired with the values of both inputs.
	 * @param values1
	 *        The values of the first input which were paired with the key.
	 * @param values2
	 *        The values of the second input which were paired with the key.
	 * @param out
	 *        A collector that collects all output pairs.
	 */
	public abstract void coGroup(IK key, Iterator<IV1> values1, Iterator<IV2> values2, Collector<OK, OV> out);

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initTypes() {
		super.firstIK = getTemplateType1(getClass());
		super.firstIV = getTemplateType2(getClass());
		super.secondIK = getTemplateType1(getClass());
		super.secondIV = getTemplateType3(getClass());
		super.ok = getTemplateType4(getClass());
		super.ov = getTemplateType5(getClass());
	}

}
