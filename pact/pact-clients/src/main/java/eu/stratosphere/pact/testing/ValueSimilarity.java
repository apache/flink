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

import eu.stratosphere.pact.common.type.Value;

/**
 * Distance function between two values.
 * 
 * @author Arvid Heise
 * @param <V>
 *        the value type
 */
public interface ValueSimilarity<V extends Value> {
	/**
	 * Constant used to indicate that two values do not match.
	 */
	public static double NO_MATCH = -1;

	/**
	 * Returns the distance between the first and the second value or {@link #NO_MATCH}.
	 * 
	 * @param value1
	 *        the first value
	 * @param value2
	 *        the second value
	 * @return a positive value corresponding to the distance or {@link #NO_MATCH}
	 */
	public double getDistance(V value1, V value2);

	public boolean isApplicable(Class<? extends V> valueType);
}