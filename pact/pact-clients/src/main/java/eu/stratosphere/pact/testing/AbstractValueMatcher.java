/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.testing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Simple matching algorithm that returns unmatched values but allows a value from one bag to be matched several times
 * against items from another bag.
 * 
 * @author Arvid.Heise
 * @param <PactRecord>
 */
public abstract class AbstractValueMatcher implements FuzzyValueMatcher {
	/**
	 * Calculates the overall distance between the expected and actual record.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected double getDistance(Class<? extends Value>[] schema, Int2ObjectMap<List<ValueSimilarity<?>>> similarities,
			PactRecord expectedRecord, PactRecord actualRecord) {
		double distance = 0;
		for (int index = 0; index < schema.length; index++) {
			Value expected = expectedRecord.getField(index, schema[index]);
			Value actual = actualRecord.getField(index, schema[index]);

			List<ValueSimilarity<?>> sims = similarities.get(index);
			if (sims == null) {
				if (!Equaler.SafeEquals.equal(actual, expected) )
					return ValueSimilarity.NO_MATCH;
				continue;
			}

			double valueDistance = 0;
			for (ValueSimilarity sim : sims) {
				double simDistance = sim.getDistance(expected, actual);
				if (simDistance < 0)
					return simDistance;
				valueDistance += simDistance;
			}
			distance += valueDistance / sims.size();
		}
		return distance;
	}
}
