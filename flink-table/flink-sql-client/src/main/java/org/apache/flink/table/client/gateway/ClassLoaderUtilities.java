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

package org.apache.flink.table.client.gateway;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;

/** Utilities for dynamic load jar. */
public class ClassLoaderUtilities {

    public static ClassLoader addToClassPath(URLClassLoader loader, List<URL> urls) {
        if (useExistingClassLoader(loader)) {
            final JarResourceClassLoader udfClassLoader = (JarResourceClassLoader) loader;
            for (URL url : urls) {
                udfClassLoader.addURL(url);
            }
            return udfClassLoader;
        } else {
            return createJARClassLoader(loader, urls);
        }
    }

    public static ClassLoader createJARClassLoader(URLClassLoader loader, List<URL> urls) {
        final Set<URL> curPathsSet = Sets.newHashSet(loader.getURLs());
        final List<URL> curPaths = Lists.newArrayList(curPathsSet);
        for (URL oneurl : urls) {
            if (oneurl != null && !curPathsSet.contains(oneurl)) {
                curPaths.add(oneurl);
            }
        }
        return new JarResourceClassLoader(curPaths.toArray(new URL[0]), loader);
    }

    private static boolean useExistingClassLoader(ClassLoader cl) {
        if (!(cl instanceof JarResourceClassLoader)) {
            // Cannot use the same classloader if it is not an instance of {@code
            // JarResourceClassLoader}
            return false;
        }
        final JarResourceClassLoader udfClassLoader = (JarResourceClassLoader) cl;
        if (udfClassLoader.isClosed()) {
            // The classloader may have been closed, Cannot add to the same instance
            return false;
        }
        return true;
    }
}
