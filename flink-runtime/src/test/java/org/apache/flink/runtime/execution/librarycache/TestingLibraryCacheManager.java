/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.JobID;

import java.util.function.BiFunction;

/** Testing {@link LibraryCacheManager} implementation. */
public class TestingLibraryCacheManager implements LibraryCacheManager {
    private final BiFunction<JobID, Boolean, ClassLoaderLease> registerOrRetainClassLoaderFunction;
    private final Runnable shutdownRunnable;
    private final boolean wrapsSystemClassLoader;

    private TestingLibraryCacheManager(
            BiFunction<JobID, Boolean, LibraryCacheManager.ClassLoaderLease>
                    registerOrRetainClassLoaderFunction,
            Runnable shutdownRunnable,
            boolean wrapsSystemClassLoader) {
        this.registerOrRetainClassLoaderFunction = registerOrRetainClassLoaderFunction;
        this.shutdownRunnable = shutdownRunnable;
        this.wrapsSystemClassLoader = wrapsSystemClassLoader;
    }

    @Override
    public LibraryCacheManager.ClassLoaderLease registerClassLoaderLease(JobID jobId) {
        return registerOrRetainClassLoaderFunction.apply(jobId, wrapsSystemClassLoader);
    }

    @Override
    public void shutdown() {
        shutdownRunnable.run();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private Runnable shutdownRunnable = () -> {};
        private BiFunction<JobID, Boolean, LibraryCacheManager.ClassLoaderLease>
                registerOrRetainClassLoaderFunction =
                        (ignored1, ignore2) -> TestingClassLoaderLease.newBuilder().build();

        private boolean wrapsSystemClassLoader = false;

        private Builder() {}

        public Builder setShutdownRunnable(Runnable shutdownRunnable) {
            this.shutdownRunnable = shutdownRunnable;
            return this;
        }

        public Builder setRegisterOrRetainClassLoaderFunction(
                BiFunction<JobID, Boolean, ClassLoaderLease> registerOrRetainClassLoaderFunction) {
            this.registerOrRetainClassLoaderFunction = registerOrRetainClassLoaderFunction;
            return this;
        }

        public Builder setWrapsSystemClassLoader(boolean wrapsSystemClassLoader) {
            this.wrapsSystemClassLoader = wrapsSystemClassLoader;
            return this;
        }

        public TestingLibraryCacheManager build() {
            return new TestingLibraryCacheManager(
                    registerOrRetainClassLoaderFunction, shutdownRunnable, wrapsSystemClassLoader);
        }
    }
}
