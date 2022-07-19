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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;

import java.net.URL;
import java.net.URLClassLoader;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;

/** URL class loader that exposes the `addURL` method in URLClassLoader. */
@Internal
public abstract class MutableURLClassLoader extends URLClassLoader {

    static {
        ClassLoader.registerAsParallelCapable();
    }

    /**
     * Creates a new instance of MutableURLClassLoader for the specified URLs, parent class loader
     * and configuration.
     */
    public static MutableURLClassLoader newInstance(
            final URL[] urls, final ClassLoader parent, final Configuration configuration) {
        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(configuration);
        final String classLoaderResolveOrder =
                configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
        final FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
                FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
        final boolean checkClassloaderLeak =
                configuration.getBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER);
        return FlinkUserCodeClassLoaders.create(
                resolveOrder,
                urls,
                parent,
                alwaysParentFirstLoaderPatterns,
                NOOP_EXCEPTION_HANDLER,
                checkClassloaderLeak);
    }

    public MutableURLClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
    }
}
