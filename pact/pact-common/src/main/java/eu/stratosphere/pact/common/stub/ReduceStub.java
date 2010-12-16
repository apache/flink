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

import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Pair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.KeyGroupedIterator;

/**
 * The ReduceStub must be extended to provide a reducer implementation which is
 * called by a Reduce PACT. By definition, the Reduce PACT calls the reduce
 * implementation once for each distinct key and all values that come with that key.
 * For details on the Reduce PACT read the documentation of the PACT programming model.
 * <p>
 * The ReduceStub extension must be parameterized with the types of its input and output keys and values.
 * <p>
 * For a reduce implementation, the <code>reduce()</code> method must be implemented.
 * 
 * @author Fabian Hueske
 */
public abstract class ReduceStub<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		SingleInputStub<IK, IV, OK, OV> {

	/**
	 * This method is currently final. We might make it available for overwriting in the future.
	 * The top entry point into the reducing functions. By default, this method goes over all keys and values
	 * that are to be processed by its instance of the reducing code and calls the <code>reduce()</code> function
	 * for each key separately. In most cases, this function will not be changed by the programmer that implements
	 * a reducer. It may however be overridden, if the programmer wishes to have a view over to data with a
	 * scope that stretches across a single key. This function sees all data that is processed in its instance
	 * of the reducing code in one function call.
	 * <p>
	 * Which keys and values are processed by this function depends on the distribution of the keys across the
	 * instances.
	 * 
	 * @param in
	 *        An iterator over all key/value pairs processed by this instance of the reducing code.
	 *        The pairs are grouped by key, such that equal keys are always in a contiguous sequence.
	 * @param out
	 *        The collector to write the results to.
	 */
	public final void run(Iterator<Pair<IK, IV>> in, Collector<OK, OV> out) {
		KeyGroupedIterator<IK, IV> iter = new KeyGroupedIterator<IK, IV>(in);
		while (iter.nextKey()) {
			reduce(iter.getKey(), iter.getValues(), out);
		}
	}

	/**
	 * The central function to be implemented for a reducer. The function receives per call one
	 * key and all the values that belong to that key. Each key is guaranteed to be processed by exactly
	 * one function call across all involved instances across all computing nodes.
	 * 
	 * @param key
	 *        The input key.
	 * @param values
	 *        All values that belong to the given input key.
	 * @param out
	 *        The collector to hand results to.
	 */
	public abstract void reduce(IK key, Iterator<IV> values, Collector<OK, OV> out);

	/**
	 * No default implementation provided.
	 * This method must be overridden by reduce stubs that want to make use of the combining feature.
	 * In addition, the ReduceStub extending class must be annotated as Combinable.
	 * Note that this function must be implemented, if the reducer is annotated as combinable.
	 * <p>
	 * The use of the combiner is typically a pre-reduction of the data. It works similar as the reducer, only that is
	 * is not guaranteed to see all values with the same key in one call to the combine function. Since it is called
	 * prior to the <code>reduce()</code> method, input and output types of the combine method are the input types of
	 * the <code>reduce()</code> method.
	 * 
	 * @see eu.stratosphere.pact.common.contract.ReduceContract.Combinable
	 * @param key
	 *        The key to combine values for.
	 * @param values
	 *        The values to be combined. Unlike in the reduce method, these are not necessarily all values
	 *        belonging to the given key.
	 * @param out
	 *        The collector to write the result to.
	 */
	public void combine(IK key, Iterator<IV> values, Collector<IK, IV> out) {
		// to be implemented, if the reducer should use a combiner. Note that the combining method
		// is only used, if the stub class is further annotated with the annotation
		// @ReduceContract.Combinable
	}

}
