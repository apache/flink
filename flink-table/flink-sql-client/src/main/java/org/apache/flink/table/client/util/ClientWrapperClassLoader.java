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

package org.apache.flink.table.client.util;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkUserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.FlinkUserCodeClassLoaders.SafetyNetWrapperClassLoader;

/**
 * This class loader extends {@link SafetyNetWrapperClassLoader}, upon the {@code addURL} method, it
 * also exposes a {@code removeURL} method which used to remove unnecessary jar from current
 * classloader path. This class loader wraps a {@link FlinkUserCodeClassLoader} and an old
 * classloader list, the class load is delegated to the inner {@link FlinkUserCodeClassLoader}.
 *
 * <p>This is only used to SqlClient for supporting {@code REMOVE JAR} clause currently. When remove
 * a jar, get the registered jar url list from current {@link FlinkUserCodeClassLoader} firstly,
 * then create a new instance of {@link FlinkUserCodeClassLoader} which urls doesn't include the
 * removed jar, and the currentClassLoader point to new instance object, the old object is added to
 * list to be closed when close {@link ClientWrapperClassLoader}.
 *
 * <p>Note: This classloader is not guaranteed to actually remove class or resource, any classes or
 * resources in the removed jar that are already loaded, are still accessible.
 */
@Experimental
@Internal
public class ClientWrapperClassLoader extends SafetyNetWrapperClassLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWrapperClassLoader.class);

    static {
        ClassLoader.registerAsParallelCapable();
    }

    private final Configuration configuration;
    private final List<FlinkUserCodeClassLoader> originClassLoaders;

    public ClientWrapperClassLoader(FlinkUserCodeClassLoader inner, Configuration configuration) {
        super(inner, inner.getParent());
        this.configuration = new Configuration(configuration);
        this.originClassLoaders = new ArrayList<>();
    }

    public void removeURL(URL url) {
        Set<URL> registeredUrls = Stream.of(inner.getURLs()).collect(Collectors.toSet());
        if (!registeredUrls.contains(url)) {
            LOG.warn(
                    String.format(
                            "Could not remove the specified jar because the jar path [%s] is not found in classloader.",
                            url));
            return;
        }

        originClassLoaders.add(inner);
        // build a new classloader without removed jars
        registeredUrls.remove(url);
        inner =
                ClientClassloaderUtil.buildUserClassLoader(
                        new ArrayList<>(registeredUrls),
                        ClientWrapperClassLoader.class.getClassLoader(),
                        configuration);
    }

    @Override
    public void close() {
        super.close();
        // close other classloader in the list
        for (FlinkUserCodeClassLoader classLoader : originClassLoaders) {
            try {
                classLoader.close();
            } catch (IOException e) {
                LOG.error("Failed to close the origin classloader.", e);
            }
        }

        originClassLoaders.clear();
    }
}
