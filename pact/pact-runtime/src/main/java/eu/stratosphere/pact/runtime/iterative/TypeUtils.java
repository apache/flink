/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class TypeUtils {

	private TypeUtils() {
	}

	public static <T> TypeSerializer<T> instantiateTypeSerializer(
			Class<? extends TypeSerializerFactory<?>> serializerFactoryClass) {
		final TypeSerializerFactory<?> serializerFactory;
		if (serializerFactoryClass == null) {
			// fall back to PactRecord
			serializerFactory = PactRecordSerializerFactory.get();
		} else {
			serializerFactory = InstantiationUtil.instantiate(serializerFactoryClass, TypeSerializerFactory.class);
		}
		return (TypeSerializer<T>) serializerFactory.getSerializer();
	}

	public static <T> TypeComparator<T> instantiateTypeComparator(Configuration config,
			ClassLoader userCodeClassLoader, Class<? extends TypeComparatorFactory<?>> comparatorFactoryClass, String keyPrefix)
			throws ClassNotFoundException {
		final TypeComparatorFactory<?> comparatorFactory;
		if (comparatorFactoryClass == null) {
			// fall back to PactRecord
			comparatorFactory = PactRecordComparatorFactory.get();
		} else {
			comparatorFactory = InstantiationUtil.instantiate(comparatorFactoryClass, TypeComparatorFactory.class);
		}
		return (TypeComparator<T>) comparatorFactory.createComparator(
			new TaskConfig.DelegatingConfiguration(config, keyPrefix), userCodeClassLoader);
	}

	public static <T1, T2> TypePairComparatorFactory<T1, T2> instantiateTypePairComparator(
			Class<? extends TypePairComparatorFactory<T1, T2>> comparatorFactoryClass) throws ClassNotFoundException {

		TypePairComparatorFactory<T1, T2> pairComparatorFactory;
		if (comparatorFactoryClass == null) {
			@SuppressWarnings("unchecked")
			TypePairComparatorFactory<T1, T2> pactRecordFactory =
				(TypePairComparatorFactory<T1, T2>) PactRecordPairComparatorFactory.get();
			pairComparatorFactory = pactRecordFactory;
		} else {
			@SuppressWarnings("unchecked")
			final Class<TypePairComparatorFactory<T1, T2>> clazz =
				(Class<TypePairComparatorFactory<T1, T2>>) (Class<?>) TypePairComparatorFactory.class;
			pairComparatorFactory = InstantiationUtil.instantiate(comparatorFactoryClass, clazz);
		}
		return pairComparatorFactory;
	}
}
