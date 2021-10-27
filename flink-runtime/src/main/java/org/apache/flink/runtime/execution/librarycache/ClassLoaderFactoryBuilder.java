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
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import javax.annotation.Nullable;

import java.util.function.Consumer;



/**
 * Provides facilities to customize {@link ClassLoaderFactory} by user ,it is instantinate by ServiceLoader.
 * @see java.util.ServiceLoader
 */
public interface ClassLoaderFactoryBuilder {

    public default ClassLoaderFactory buildServerLoaderFactory(
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
            String[] alwaysParentFirstPatterns,
            @Nullable Consumer<Throwable> exceptionHander,
            boolean checkClassLoaderLeak){
        throw new UnsupportedOperationException();
    }

    public default ClassLoaderFactory buildClientLoaderFactory(
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
            String[] alwaysParentFirstPatterns,
            @Nullable Consumer<Throwable> exceptionHander,
            boolean checkClassLoaderLeak){
        throw new UnsupportedOperationException();
    }
}
