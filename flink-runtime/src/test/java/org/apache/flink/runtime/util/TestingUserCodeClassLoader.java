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

package org.apache.flink.runtime.util;

import org.apache.flink.util.UserCodeClassLoader;

import java.util.function.BiConsumer;

/** Testing implementation of {@link UserCodeClassLoader}. */
public class TestingUserCodeClassLoader implements UserCodeClassLoader {

    private final ClassLoader classLoader;

    private final BiConsumer<String, Runnable> registerReleaseHookConsumer;

    public TestingUserCodeClassLoader(
            ClassLoader classLoader, BiConsumer<String, Runnable> registerReleaseHookConsumer) {
        this.classLoader = classLoader;
        this.registerReleaseHookConsumer = registerReleaseHookConsumer;
    }

    @Override
    public ClassLoader asClassLoader() {
        return classLoader;
    }

    @Override
    public void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
        registerReleaseHookConsumer.accept(releaseHookName, releaseHook);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the testing classloader. */
    public static final class Builder {
        private ClassLoader classLoader = Builder.class.getClassLoader();
        private BiConsumer<String, Runnable> registerReleaseHookConsumer = (ign, ore) -> {};

        private Builder() {}

        public Builder setClassLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public Builder setRegisterReleaseHookConsumer(
                BiConsumer<String, Runnable> registerReleaseHookConsumer) {
            this.registerReleaseHookConsumer = registerReleaseHookConsumer;
            return this;
        }

        public TestingUserCodeClassLoader build() {
            return new TestingUserCodeClassLoader(classLoader, registerReleaseHookConsumer);
        }
    }
}
