/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.security.factories;

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * The Service provider discovery for searching suitable {@link SecurityFactory}
 * based on provided requirements.
 */
public class SecurityFactoryService {

	private static ServiceLoader<SecurityFactory> defaultLoader =
		ServiceLoader.load(SecurityFactory.class);

	/**
	 * Find a suitable {@link SecurityModuleFactory} based on canonical name.
	 */
	public static SecurityModuleFactory findModuleFactory(String securityModuleFactoryClass) throws NoMatchSecurityFactoryException {
		return (SecurityModuleFactory) findFactoryInternal(
			securityModuleFactoryClass,
			SecurityModuleFactory.class.getClassLoader());
	}

	/**
	 * Find a suitable {@link SecurityContextFactory} based on canonical name.
	 */
	public static SecurityContextFactory findContextFactory(String securityContextFactoryClass) throws NoMatchSecurityFactoryException {
		return (SecurityContextFactory) findFactoryInternal(
			securityContextFactoryClass,
			SecurityContextFactory.class.getClassLoader());
	}

	private static SecurityFactory findFactoryInternal(
		String factoryClassCanonicalName,
		ClassLoader classLoader) throws NoMatchSecurityFactoryException {

		Preconditions.checkNotNull(factoryClassCanonicalName);

		ServiceLoader<SecurityFactory> serviceLoader = null;

		if (classLoader != null) {
			serviceLoader = ServiceLoader.load(SecurityFactory.class, classLoader);
		} else {
			serviceLoader = defaultLoader;
		}

		List<SecurityFactory> matchingFactories = new ArrayList<>();
		Iterator<SecurityFactory> classFactoryIterator = serviceLoader.iterator();
		classFactoryIterator.forEachRemaining(classFactory -> {
			if (factoryClassCanonicalName.matches(classFactory.getClass().getCanonicalName())) {
				matchingFactories.add(classFactory);
			}
		});

		if (matchingFactories.isEmpty() || matchingFactories.size() > 1) {
			throw new NoMatchSecurityFactoryException(
				"zero or more than one security factory found",
				factoryClassCanonicalName,
				matchingFactories
			);
		}
		return matchingFactories.get(0);
	}

}
