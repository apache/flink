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

package eu.stratosphere.nephele.services.iomanager;

/**
 * <p>
 * Encapsulates a {@link Serializer}/{@link Deserializer} pair. Typically the factory implementation is bound to a
 * specified Class<T> object. This allows components to instantiate (De-)Serializer objects without having the Class<T>
 * object at hand.
 * </p>
 * 
 * @author Erik Nijkamp
 * @param <T>
 */
public interface SerializationFactory<T> {
	/**
	 * @return a {@link Serializer} for the given class.
	 */
	Serializer<T> getSerializer();

	/**
	 * @return a {@link Deserializer} for the given class.
	 */
	Deserializer<T> getDeserializer();

	/**
	 * @return a new instance of the bound class object
	 */
	T newInstance();

	/**
	 * Creates a raw comparator for the given type. If none is available, this method returns
	 * null.
	 * 
	 * @return A raw comparator, or null, if none is available.
	 */
	RawComparator getRawComparator();
}
