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
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType6;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * The CrossStub must be extended to provide a cross implementation which is
 * called by a Cross PACT. By definition, a Cross PACT has two input sets of
 * key-value pairs. It calls the cross implementation for each element of the
 * Cartesian product of both input sets. For details on the Cross PACT read the
 * documentation of the PACT programming model.
 * <p>
 * The CrossStub extension must be parametrized with the types of the keys and values of its both inputs and output.
 * <p>
 * For a cross implementation, the <code>cross()</code> method must be implemented.
 * 
 * @author Fabian Hueske
 * @param <IK1>
 *        Type of the first input's key.
 * @param <IV1>
 *        Type of the first input's value.
 * @param <IK2>
 *        Type of the second input's key.
 * @param <IV2>
 *        Type of the second input's value.
 * @param <OK>
 *        Type of the output key.
 * @param <OV>
 *        Type of the output value.
 */
public abstract class CrossStub<IK1 extends Key, IV1 extends Value, IK2 extends Key, IV2 extends Value, OK extends Key, OV extends Value>
		extends DualInputStub<IK1, IV1, IK2, IV2, OK, OV> {

	/**
	 * This method must be implemented to provide a user implementation of a cross.
	 * It is called for each element of the Cartesian product of both input sets.
	 * 
	 * @param key1
	 *        The key of a pair of the first input.
	 * @param value1
	 *        The value of a pair of the second input.
	 * @param key2
	 *        The key of a pair of the second input.
	 * @param value2
	 *        The value of a pair of the second input.
	 * @param out
	 *        A collector that collects all output pairs.
	 */
	public abstract void cross(IK1 key1, IV1 value1, IK2 key2, IV2 value2, Collector<OK, OV> out);

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initTypes() {
		super.firstIK = getTemplateType1(getClass());
		super.firstIV = getTemplateType2(getClass());
		super.secondIK = getTemplateType3(getClass());
		super.secondIV = getTemplateType4(getClass());
		super.ok = getTemplateType5(getClass());
		super.ov = getTemplateType6(getClass());
	}
}
