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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager.ClassLoaderFactory;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders.ResolveOrder;

import javax.annotation.Nullable;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.Consumer;

/** Testing implementation of {@link ClassLoaderFactoryBuilder}. */
public class TestingClassLoaderFactoryBuilder implements ClassLoaderFactoryBuilder {
    public static final String TESTING_CLASSLOADER_FACTORY_BUILDER_ENABLE =
            "testing_classloader_factory_builder_enable";

    @Override
    public boolean isCompatible() {
        return Boolean.parseBoolean(
                System.getProperties().getProperty(TESTING_CLASSLOADER_FACTORY_BUILDER_ENABLE));
    }

    @Override
    public ClassLoaderFactory buildServerLoaderFactory(
            ResolveOrder classLoaderResolveOrder,
            String[] alwaysParentFirstPatterns,
            @Nullable Consumer<Throwable> exceptionHander,
            boolean checkClassLoaderLeak) {
      return new TestingClassLoaderFactory();
    }

    @Override
    public ClassLoaderFactory buildClientLoaderFactory(
            ResolveOrder classLoaderResolveOrder,
            String[] alwaysParentFirstPatterns,
            @Nullable Consumer<Throwable> exceptionHander,
            boolean checkClassLoaderLeak) {
        return new TestingClassLoaderFactory();
    }


    public static class TestingClassLoaderFactory implements ClassLoaderFactory{
        @Override
        public URLClassLoader createClassLoader(URL[] libraryURLs) {
            throw new UnsupportedOperationException();
        }
    }
}
