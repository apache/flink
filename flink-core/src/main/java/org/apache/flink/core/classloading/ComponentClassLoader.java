/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.classloading;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link URLClassLoader} that restricts which classes can be loaded to those contained within the
 * given classpath, except classes from a given set of packages that are either loaded owner or
 * component-first.
 *
 * <p>Depiction of the class loader hierarchy:
 *
 * <pre>
 *       Owner     Bootstrap
 *           ^         ^
 *           |---------|
 *                |
 *            Component
 * </pre>
 *
 * <p>For loading classes/resources, class loaders are accessed in one of the following orders:
 *
 * <ul>
 *   <li>component-only: component -> bootstrap; default.
 *   <li>component-first: component -> bootstrap -> owner; opt-in.
 *   <li>owner-first: owner -> component -> bootstrap; opt-in.
 * </ul>
 */
public class ComponentClassLoader extends URLClassLoader {
    private static final ClassLoader PLATFORM_OR_BOOTSTRAP_LOADER;

    private final ClassLoader ownerClassLoader;

    private final String[] ownerFirstPackages;
    private final String[] componentFirstPackages;
    private final String[] ownerFirstResourcePrefixes;
    private final String[] componentFirstResourcePrefixes;

    private final Map<String, String> knownPackagePrefixesModuleAssociation;

    public ComponentClassLoader(
            URL[] classpath,
            ClassLoader ownerClassLoader,
            String[] ownerFirstPackages,
            String[] componentFirstPackages,
            Map<String, String> knownPackagePrefixesModuleAssociation) {
        super(classpath, PLATFORM_OR_BOOTSTRAP_LOADER);
        this.ownerClassLoader = ownerClassLoader;

        this.ownerFirstPackages = ownerFirstPackages;
        this.componentFirstPackages = componentFirstPackages;

        this.knownPackagePrefixesModuleAssociation = knownPackagePrefixesModuleAssociation;

        ownerFirstResourcePrefixes = convertPackagePrefixesToPathPrefixes(ownerFirstPackages);
        componentFirstResourcePrefixes =
                convertPackagePrefixesToPathPrefixes(componentFirstPackages);
    }

    // ----------------------------------------------------------------------------------------------
    // Class loading
    // ----------------------------------------------------------------------------------------------

    @Override
    protected Class<?> loadClass(final String name, final boolean resolve)
            throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            try {
                final Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass != null) {
                    return resolveIfNeeded(resolve, loadedClass);
                }

                if (isComponentFirstClass(name)) {
                    return loadClassFromComponentFirst(name, resolve);
                }
                if (isOwnerFirstClass(name)) {
                    return loadClassFromOwnerFirst(name, resolve);
                }

                // making this behavior configurable (component-only/component-first/owner-first)
                // would allow this class to subsume the FlinkUserCodeClassLoader (with an added
                // exception handler)
                return loadClassFromComponentOnly(name, resolve);
            } catch (ClassNotFoundException e) {
                // If we know the package of this class
                Optional<String> foundAssociatedModule =
                        knownPackagePrefixesModuleAssociation.entrySet().stream()
                                .filter(entry -> name.startsWith(entry.getKey()))
                                .map(Map.Entry::getValue)
                                .findFirst();
                if (foundAssociatedModule.isPresent()) {
                    throw new ClassNotFoundException(
                            String.format(
                                    "Class '%s' not found. Perhaps you forgot to add the module '%s' to the classpath?",
                                    name, foundAssociatedModule.get()),
                            e);
                }
                throw e;
            }
        }
    }

    private Class<?> resolveIfNeeded(final boolean resolve, final Class<?> loadedClass) {
        if (resolve) {
            resolveClass(loadedClass);
        }
        return loadedClass;
    }

    private boolean isOwnerFirstClass(final String name) {
        return Arrays.stream(ownerFirstPackages).anyMatch(name::startsWith);
    }

    private boolean isComponentFirstClass(final String name) {
        return Arrays.stream(componentFirstPackages).anyMatch(name::startsWith);
    }

    private Class<?> loadClassFromComponentOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }

    private Class<?> loadClassFromComponentFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromComponentOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromOwnerOnly(name, resolve);
        }
    }

    private Class<?> loadClassFromOwnerOnly(final String name, final boolean resolve)
            throws ClassNotFoundException {
        return resolveIfNeeded(resolve, ownerClassLoader.loadClass(name));
    }

    private Class<?> loadClassFromOwnerFirst(final String name, final boolean resolve)
            throws ClassNotFoundException {
        try {
            return loadClassFromOwnerOnly(name, resolve);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return loadClassFromComponentOnly(name, resolve);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Resource loading
    // ----------------------------------------------------------------------------------------------

    @Override
    public URL getResource(final String name) {
        try {
            final Enumeration<URL> resources = getResources(name);
            if (resources.hasMoreElements()) {
                return resources.nextElement();
            }
        } catch (IOException ignored) {
            // mimics the behavior of the JDK
        }
        return null;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (isComponentFirstClass(name)) {
            return super.getResourceAsStream(name);
        }
        if (isOwnerFirstClass(name)) {
            return ownerClassLoader.getResourceAsStream(name);
        }
        return super.getResourceAsStream(name);
    }

    @Override
    public Enumeration<URL> getResources(final String name) throws IOException {
        if (isComponentFirstResource(name)) {
            return loadResourceFromComponentFirst(name);
        }
        if (isOwnerFirstResource(name)) {
            return loadResourceFromOwnerFirst(name);
        }

        return loadResourceFromComponentOnly(name);
    }

    private boolean isOwnerFirstResource(final String name) {
        return Arrays.stream(ownerFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    private boolean isComponentFirstResource(final String name) {
        return Arrays.stream(componentFirstResourcePrefixes).anyMatch(name::startsWith);
    }

    private Enumeration<URL> loadResourceFromComponentOnly(final String name) throws IOException {
        return super.getResources(name);
    }

    private Enumeration<URL> loadResourceFromComponentFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name, this::loadResourceFromComponentOnly, this::loadResourceFromOwnerOnly);
    }

    private Enumeration<URL> loadResourceFromOwnerOnly(final String name) throws IOException {
        return ownerClassLoader.getResources(name);
    }

    private Enumeration<URL> loadResourceFromOwnerFirst(final String name) throws IOException {
        return loadResourcesInOrder(
                name, this::loadResourceFromOwnerOnly, this::loadResourceFromComponentOnly);
    }

    private interface ResourceLoadingFunction
            extends FunctionWithException<String, Enumeration<URL>, IOException> {}

    private Enumeration<URL> loadResourcesInOrder(
            String name,
            ResourceLoadingFunction firstClassLoader,
            ResourceLoadingFunction secondClassLoader)
            throws IOException {
        final Iterator<URL> iterator =
                Iterators.concat(
                        Iterators.forEnumeration(firstClassLoader.apply(name)),
                        Iterators.forEnumeration(secondClassLoader.apply(name)));

        return new IteratorBackedEnumeration<>(iterator);
    }

    @VisibleForTesting
    static class IteratorBackedEnumeration<T> implements Enumeration<T> {
        private final Iterator<T> backingIterator;

        public IteratorBackedEnumeration(Iterator<T> backingIterator) {
            this.backingIterator = backingIterator;
        }

        @Override
        public boolean hasMoreElements() {
            return backingIterator.hasNext();
        }

        @Override
        public T nextElement() {
            return backingIterator.next();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Utils
    // ----------------------------------------------------------------------------------------------

    private static String[] convertPackagePrefixesToPathPrefixes(String[] packagePrefixes) {
        return Arrays.stream(packagePrefixes)
                .map(packageName -> packageName.replace('.', '/'))
                .toArray(String[]::new);
    }

    static {
        ClassLoader platformLoader = null;
        try {
            platformLoader =
                    (ClassLoader)
                            ClassLoader.class.getMethod("getPlatformClassLoader").invoke(null);
        } catch (NoSuchMethodException e) {
            // on Java 8 this method does not exist, but using null indicates the bootstrap
            // loader that we want to have
        } catch (Exception e) {
            throw new IllegalStateException("Cannot retrieve platform classloader on Java 9+", e);
        }
        PLATFORM_OR_BOOTSTRAP_LOADER = platformLoader;
        ClassLoader.registerAsParallelCapable();
    }
}
