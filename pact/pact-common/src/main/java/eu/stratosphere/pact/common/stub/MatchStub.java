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

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * The MatchStub must be extended to provide a matcher implementation which is
 * called by a Match PACT. By definition, a Match PACT has two input sets of
 * key-value pairs. It calls the match implementation once for each two pairs
 * that share the same key and come from different input sets. It resembles an
 * equality join of both inputs on their keys. For details on the Match PACT
 * read the documentation of the PACT programming model.
 * <p>
 * The MatchStub extension must be parametrized with the types of the keys and values of its both inputs and output.
 * <p>
 * For a match implementation, the <code>match()</code> method must be implemented.
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
public abstract class MatchStub<IK extends Key, IV1 extends Value, IV2 extends Value, OK extends Key, OV extends Value>
		extends DualInputStub<IK, IV1, IK, IV2, OK, OV> {

	/**
	 * This method is currently final. We might make it available for
	 * overwriting in the future.
	 * <p>
	 * Default implementation of the <code>run()</code> method. Calls the <code>match()</code> method for all two
	 * key-value pairs that share the same key and come from different inputs. The output of the <code>match()</code>
	 * method is forwarded.
	 * <p>
	 * This method is called with a key and two iterator (one for each input) over all values that share this key.
	 * <p>
	 * 
	 * @param key
	 *        A key.
	 * @param values1
	 *        An iterator on values of the first input that were paired with the key.
	 * @param values2
	 *        An iterator on values of the second input that were paired with the key.
	 * @param out
	 *        A collector that collects all output pairs.
	 */
	public final void run(IK key, Iterator<IV1> values1, Iterator<IV2> values2, Collector<OK, OV> out) {

		// get first value of each side
		IV1 v1 = values1.next();
		IV2 v2 = values2.next();

		if (v1 == null || v2 == null) {
			return;
		}

		boolean v1HasNext = values1.hasNext();
		boolean v2HasNext = values2.hasNext();

		// check if one side is already empty
		// this check could be omitted if we put this in MatchTask.
		// then we can derive the local strategy (with build side).
		if (!v1HasNext && !v2HasNext) {
			// both sides contain only one value
			match(key, v1, v2, out);
		} else if (!v1HasNext) {
			// only values1 contains only one value
			match(key, v1, v2, out);
			while (v2HasNext) {
				v2 = values2.next();
				v2HasNext = values2.hasNext();
				match(key, v1, v2, out);
			}

		} else if (!v2HasNext) {
			// only values2 contains only one value
			match(key, v1, v2, out);
			while (v1HasNext) {
				v1 = values1.next();
				v1HasNext = values1.hasNext();
				match(key, v1, v2, out);
			}

		} else {

			// both sides contain more than one value
			// TODO: 1) decide which side to store
			// 2) use spilling list

			// collect all values of one side
			ArrayList<IV1> values1List = new ArrayList<IV1>();
			match(key, v1, v2, out);
			values1List.add(v1);

			while (values1.hasNext()) {
				v1 = values1.next();
				match(key, v1, v2, out);
				values1List.add(v1);
			}

			// send each combination to matcher
			while (values2.hasNext()) {
				v2 = values2.next();
				for (IV1 v1List : values1List) {
					match(key, v1List, v2, out);
				}
			}
		}
	}

	/**
	 * This method must be implemented to provide a user implementation of a
	 * matcher. It is called for each two key-value pairs that share the same
	 * key and come from different inputs.
	 * 
	 * @param key
	 *        The key that is shared by both pairs.
	 * @param value1
	 *        The value of the pair that comes from the first input.
	 * @param value2
	 *        The value of the pair that comes from the second input.
	 * @param out
	 *        A collector that collects all output pairs.
	 */
	public abstract void match(IK key, IV1 value1, IV2 value2, Collector<OK, OV> out);

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
