/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.java.typeutils.runtime;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.InstantiationUtil;

public final class RuntimeComparatorFactory<T> implements TypeComparatorFactory<T>, java.io.Serializable {

	private static final long serialVersionUID = 1L;


	private static final String CONFIG_KEY = "SER_DATA";

	private TypeComparator<T> comparator;


	public RuntimeComparatorFactory() {}

	public RuntimeComparatorFactory(TypeComparator<T> comparator) {
		this.comparator = comparator;
	}

	@Override
	public void writeParametersToConfig(Configuration config) {
		try {
			InstantiationUtil.writeObjectToConfig(comparator, config, CONFIG_KEY);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not serialize comparator into the configuration.", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
		try {
			comparator = (TypeComparator<T>) InstantiationUtil.readObjectFromConfig(config, CONFIG_KEY, cl);
		}
		catch (ClassNotFoundException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException("Could not serialize serializer into the configuration.", e);
		}
	}

	@Override
	public TypeComparator<T> createComparator() {
		if (comparator != null) {
			return comparator;
		} else {
			throw new RuntimeException("ComparatorFactory has not been initialized from configuration.");
		}
	}
}
