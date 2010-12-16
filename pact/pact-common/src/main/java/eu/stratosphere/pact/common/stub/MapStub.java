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

/**
 * The MapStub must be extended to provide a mapper implementation which is
 * called by a Map PACT. By definition, the Map PACT calls the mapper
 * implementation for each individual input key-value pair. For details on the
 * Map PACT read the documentation of the PACT programming model.
 * <p>
 * The MapStub extension must be parametrized with the types of its input and output keys and values.
 * <p>
 * For a mapper implementation, the <code>map()</code> method must be implemented.
 * 
 * @author Fabian Hueske
 * @param <IK>
 *        The type of the input key
 * @param <IV>
 *        The type of the input value
 * @param <OK>
 *        The type of the output key
 * @param <OV>
 *        The type of the output value
 */
public abstract class MapStub<IK extends Key, IV extends Value, OK extends Key, OV extends Value> extends
		SingleInputStub<IK, IV, OK, OV> {

	/**
	 * This method is currently final. We might make it available for overwriting in the future.
	 * <p>
	 * Default implementation of the <code>run()</code> method. Calls the <code>map()</code> method for each individual
	 * input key-value pair and forwards its output.
	 * <p>
	 * This method is called with an iterator over all k-v pairs that this thread will process. The input data of this
	 * method is not deterministic.
	 * 
	 * @param in
	 *        Iterator over all key-value pairs that this thread will
	 *        process
	 * @param out
	 *        A collector for the output of the mapper.
	 */
	public final void run(Iterator<Pair<IK, IV>> in, Collector<OK, OV> out) {
		while (in.hasNext()) {
			Pair<IK, IV> pair = in.next();
			map(pair.getKey(), pair.getValue(), out);
		}
	}

	/**
	 * This method must be implemented to provide a user implementation of a mapper.
	 * It is called for each individual key-value pair.
	 * 
	 * @param key
	 *        The key of the input pair.
	 * @param value
	 *        The value of the input pair.
	 * @param out
	 *        A collector that collects all output pairs.
	 */
	protected abstract void map(IK key, IV value, Collector<OK, OV> out);

}
