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

package org.apache.flink.runtime.security;

import org.apache.flink.runtime.security.contexts.SecurityContextFactory;
import org.apache.flink.runtime.security.modules.SecurityModuleFactory;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * The Service provider discovery for searching suitable security factory.
 *
 * <p>It could discover either {@link SecurityContextFactory} or {@link SecurityModuleFactory},
 * based on provided requirements.
 */
public class SecurityFactoryServiceLoader {

    /** Find a suitable {@link SecurityModuleFactory}s based on canonical name. */
    public static SecurityModuleFactory findModuleFactory(String securityModuleFactoryClass)
            throws NoMatchSecurityFactoryException {
        return findFactoryInternal(
                securityModuleFactoryClass,
                SecurityModuleFactory.class,
                SecurityModuleFactory.class.getClassLoader());
    }

    /** Find a suitable {@link SecurityContextFactory} based on canonical name. */
    public static SecurityContextFactory findContextFactory(String securityContextFactoryClass)
            throws NoMatchSecurityFactoryException {
        return findFactoryInternal(
                securityContextFactoryClass,
                SecurityContextFactory.class,
                SecurityContextFactory.class.getClassLoader());
    }

    private static <T> T findFactoryInternal(
            String factoryClassCanonicalName, Class<T> factoryClass, ClassLoader classLoader)
            throws NoMatchSecurityFactoryException {

        Preconditions.checkNotNull(factoryClassCanonicalName);

        ServiceLoader<T> serviceLoader;
        if (classLoader != null) {
            serviceLoader = ServiceLoader.load(factoryClass, classLoader);
        } else {
            serviceLoader = ServiceLoader.load(factoryClass);
        }

        List<T> matchingFactories = new ArrayList<>();
        Iterator<T> classFactoryIterator = serviceLoader.iterator();
        classFactoryIterator.forEachRemaining(
                classFactory -> {
                    if (factoryClassCanonicalName.matches(
                            classFactory.getClass().getCanonicalName())) {
                        matchingFactories.add(classFactory);
                    }
                });

        if (matchingFactories.size() != 1) {
            throw new NoMatchSecurityFactoryException(
                    "zero or more than one security factory found",
                    factoryClassCanonicalName,
                    matchingFactories,
                    null);
        }
        return matchingFactories.get(0);
    }

    /** Find suitable {@link SecurityModuleFactory}s based on canonical names. */
    public static List<SecurityModuleFactory> findModuleFactory(
            List<String> securityModuleFactoryClasses) throws NoMatchSecurityFactoryException {
        return findFactoryInternal(
                securityModuleFactoryClasses,
                SecurityModuleFactory.class,
                SecurityModuleFactory.class.getClassLoader());
    }

    private static <T> List<T> findFactoryInternal(
            List<String> factoryClassCanonicalNames, Class<T> factoryClass, ClassLoader classLoader)
            throws NoMatchSecurityFactoryException {
        Preconditions.checkArgument(!factoryClassCanonicalNames.isEmpty());
        factoryClassCanonicalNames.forEach(className -> Preconditions.checkNotNull(className));

        ServiceLoader<T> serviceLoader;
        if (classLoader != null) {
            serviceLoader = ServiceLoader.load(factoryClass, classLoader);
        } else {
            serviceLoader = ServiceLoader.load(factoryClass);
        }

        Set<String> factoriesSet = new HashSet<>(factoryClassCanonicalNames);
        List<T> matchingFactories = new ArrayList<>();
        Iterator<T> classFactoryIterator = serviceLoader.iterator();
        classFactoryIterator.forEachRemaining(
                classFactory -> {
                    String factoryClassCanonicalName = classFactory.getClass().getCanonicalName();
                    if (factoriesSet.contains(factoryClassCanonicalName)) {
                        matchingFactories.add(classFactory);
                        factoriesSet.remove(factoryClassCanonicalName);
                    }
                });

        if (matchingFactories.size() != factoryClassCanonicalNames.size()) {
            throw new NoMatchSecurityFactoryException(
                    "zero or more than one security factory found",
                    factoriesSet,
                    matchingFactories);
        }
        return matchingFactories;
    }
}
