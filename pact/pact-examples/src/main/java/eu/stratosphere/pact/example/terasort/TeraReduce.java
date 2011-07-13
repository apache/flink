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

package eu.stratosphere.pact.example.terasort;

import java.util.Iterator;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;

/**
 * This is a stub class implementing a PACT Reduce contract. Its only purpose is to forward the sorted key-value pairs.
 * 
 * @author warneke
 */
public final class TeraReduce extends ReduceStub<TeraKey, TeraValue, TeraKey, TeraValue> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reduce(TeraKey key, Iterator<TeraValue> values, Collector<TeraKey, TeraValue> out) {

		while (values.hasNext()) {
			out.collect(key, values.next());
		}

	}

}
