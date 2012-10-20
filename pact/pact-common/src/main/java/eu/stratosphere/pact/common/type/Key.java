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

package eu.stratosphere.pact.common.type;

/**
 * This interface has to be implemented by all data types that act as key. Keys are used to establish
 * relationships between values. A key must always be {@link java.lang.Comparable} to other keys of
 * the same type. In addition, keys must implement a correct {@link java.lang.Object#hashCode()} method
 * and {@link java.lang.Object#equals(Object)} method to ensure that grouping on keys works properly.
 * <p>
 * This interface extends {@link eu.stratosphere.pact.common.type.Value} and requires to implement
 * the serialization of its value.
 * 
 * @see eu.stratosphere.pact.common.type.Value
 * @see eu.stratosphere.nephele.types.Record
 * @see java.lang.Comparable
 */
public interface Key extends Value, Comparable<Key>
{
	/**
	 * All keys must override the hash-code function to generate proper deterministic hash codes,
	 * based on their contents.
	 * 
	 * @return The hash code of the key
	 */
	public int hashCode();
	
	/**
	 * @param other
	 * @return
	 */
	public boolean equals(Object other);
}
