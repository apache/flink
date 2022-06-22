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

package org.apache.flink.table.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.ClassLoaderUtil;
import org.apache.flink.util.MutableURLClassLoader;

import java.net.URL;

/** Mock implementations of {@link ResourceManager} for testing purposes. */
public class ResourceManagerMocks {

    public static ResourceManager createEmptyResourceManager() {
        Configuration configuration = new Configuration();
        final MutableURLClassLoader userClassLoader =
                ClassLoaderUtil.buildMutableURLClassLoader(
                        new URL[0], Thread.currentThread().getContextClassLoader(), configuration);
        return new ResourceManager(configuration, userClassLoader);
    }

    public static ResourceManager createResourceManager(
            URL[] urls, ClassLoader parentClassLoader, Configuration configuration) {
        final MutableURLClassLoader userClassLoader =
                ClassLoaderUtil.buildMutableURLClassLoader(urls, parentClassLoader, configuration);
        return new ResourceManager(configuration, userClassLoader);
    }

    private ResourceManagerMocks() {
        // no instantiation
    }
}
