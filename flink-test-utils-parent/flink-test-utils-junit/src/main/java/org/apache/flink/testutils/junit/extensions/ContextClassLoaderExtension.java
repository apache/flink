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

package org.apache.flink.testutils.junit.extensions;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/** JUnit extension to customize the classloader that a test is run with. */
public class ContextClassLoaderExtension implements BeforeAllCallback, AfterAllCallback {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final Function<TemporaryFolder, URLClassLoader> temporaryClassLoaderFactory;

    private ClassLoader originalClassLoader;
    private URLClassLoader temporaryClassLoader;

    private ContextClassLoaderExtension(
            Function<TemporaryFolder, URLClassLoader> temporaryClassLoaderFactory) {
        this.temporaryClassLoaderFactory = temporaryClassLoaderFactory;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        temporaryFolder.create();
        originalClassLoader = Thread.currentThread().getContextClassLoader();
        temporaryClassLoader = temporaryClassLoaderFactory.apply(temporaryFolder);
        Thread.currentThread().setContextClassLoader(temporaryClassLoader);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        temporaryClassLoader.close();
        temporaryFolder.delete();
    }

    public static ContextClassLoaderExtensionBuilder builder() {
        return new ContextClassLoaderExtensionBuilder();
    }

    /** Builder for {@link ContextClassLoaderExtension}. */
    public static class ContextClassLoaderExtensionBuilder {

        private final Collection<ServiceEntry> serviceEntries = new ArrayList<>();

        public ContextClassLoaderExtensionBuilder withServiceEntry(
                Class<?> serviceClass, String... serviceImplementations) {
            serviceEntries.add(new ServiceEntry(serviceClass, serviceImplementations));
            return this;
        }

        public ContextClassLoaderExtension build() {
            final Function<TemporaryFolder, URLClassLoader> factory =
                    temporaryFolder -> setupClassLoader(temporaryFolder, serviceEntries);

            return new ContextClassLoaderExtension(factory);
        }

        private ContextClassLoaderExtensionBuilder() {}
    }

    private static URLClassLoader setupClassLoader(
            TemporaryFolder temporaryFolder, Collection<ServiceEntry> serviceEntries) {
        final Path root = temporaryFolder.getRoot().toPath();
        try {
            writeServiceEntries(root, serviceEntries);
            final URL url = temporaryFolder.getRoot().toURI().toURL();
            return new URLClassLoader(
                    new URL[] {url}, ContextClassLoaderExtension.class.getClassLoader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeServiceEntries(Path tmpDir, Collection<ServiceEntry> serviceEntries)
            throws IOException {
        for (ServiceEntry serviceEntry : serviceEntries) {
            final Path path =
                    tmpDir.resolve(
                            Paths.get(
                                    "META-INF",
                                    "services",
                                    serviceEntry.serviceClass.getCanonicalName()));

            Files.createDirectories(path.getParent());
            Files.write(path, serviceEntry.serviceImplementations, StandardOpenOption.CREATE);
        }
    }

    private static class ServiceEntry {
        private final Class<?> serviceClass;
        private final List<String> serviceImplementations;

        public ServiceEntry(Class<?> serviceClass, String... serviceImplementations) {
            this.serviceClass = serviceClass;
            this.serviceImplementations = Arrays.asList(serviceImplementations);
        }
    }
}
