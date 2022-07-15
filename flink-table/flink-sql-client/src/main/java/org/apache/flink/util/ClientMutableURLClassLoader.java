/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class loader extends {@link MutableURLClassLoader}, upon the {@code addURL} method, it also
 * exposes a {@code removeURL} method which used to remove unnecessary jar from current classloader
 * path. This class loader wraps a {@link MutableURLClassLoader} and an old classloader list, the
 * class load is delegated to the inner {@link MutableURLClassLoader}.
 *
 * <p>This is only used to SqlClient for supporting {@code REMOVE JAR} clause currently. When remove
 * a jar, get the registered jar url list from current {@link MutableURLClassLoader} firstly, then
 * create a new instance of {@link MutableURLClassLoader} which urls doesn't include the removed
 * jar, and the currentClassLoader point to new instance object, the old object is added to list to
 * be closed when close {@link ClientMutableURLClassLoader}.
 *
 * <p>Note: This classloader is not guaranteed to actually remove class or resource, any classes or
 * resources in the removed jar that are already loaded, are still accessible.
 */
@Experimental
@Internal
public class ClientMutableURLClassLoader extends MutableURLClassLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMutableURLClassLoader.class);

    static {
        ClassLoader.registerAsParallelCapable();
    }

    private final Configuration configuration;
    private final List<MutableURLClassLoader> oldClassLoaders = new ArrayList<>();
    private MutableURLClassLoader currentClassLoader;

    public ClientMutableURLClassLoader(
            Configuration configuration, MutableURLClassLoader mutableURLClassLoader) {
        super(new URL[0], mutableURLClassLoader);
        this.configuration = new Configuration(configuration);
        this.currentClassLoader = mutableURLClassLoader;
    }

    @Override
    protected final Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return currentClassLoader.loadClass(name, resolve);
    }

    @Override
    public void addURL(URL url) {
        currentClassLoader.addURL(url);
    }

    public void removeURL(URL url) {
        Set<URL> registeredUrls =
                Stream.of(currentClassLoader.getURLs()).collect(Collectors.toSet());
        if (!registeredUrls.contains(url)) {
            LOG.warn(
                    String.format(
                            "Could not remove the specified jar because the jar path [%s] is not found in classloader.",
                            url));
            return;
        }

        // add current classloader to list
        oldClassLoaders.add(currentClassLoader);
        // remove url from registeredUrls
        registeredUrls.remove(url);
        // update current classloader point to a new MutableURLClassLoader instance
        currentClassLoader =
                MutableURLClassLoader.newInstance(
                        registeredUrls.toArray(new URL[0]),
                        currentClassLoader.getParent(),
                        configuration);
    }

    @Override
    public URL[] getURLs() {
        return currentClassLoader.getURLs();
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        try {
            // close current classloader
            currentClassLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing class loader in ClientMutableURLClassLoader.", e);
            exception = e;
        }

        // close other classloader in the list
        for (MutableURLClassLoader classLoader : oldClassLoaders) {
            try {
                classLoader.close();
            } catch (IOException ioe) {
                LOG.debug(
                        "Error while closing older class loader in ClientMutableURLClassLoader.",
                        ioe);
                exception = ExceptionUtils.firstOrSuppressed(ioe, exception);
            }
        }

        // clear the list
        oldClassLoaders.clear();

        if (exception != null) {
            throw exception;
        }
    }
}
