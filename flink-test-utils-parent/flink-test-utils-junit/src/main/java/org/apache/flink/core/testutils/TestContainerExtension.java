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

package org.apache.flink.core.testutils;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * {@code TestContainerExtension} provides common functionality for {@code TestContainer}
 * implementations.
 *
 * @param <T> The {@link GenericContainer} that shall be managed.
 */
public class TestContainerExtension<T extends GenericContainer<T>> implements CustomExtension {

    @Nullable private T testContainer;

    private final Supplier<T> testContainerCreator;

    public TestContainerExtension(Supplier<T> testContainerCreator) {
        this.testContainerCreator = testContainerCreator;
    }

    public T getTestContainer() {
        assert testContainer != null;
        return testContainer;
    }

    private void terminateTestContainer() {
        if (testContainer != null) {
            testContainer.stop();
            testContainer = null;
        }
    }

    private void instantiateTestContainer() {
        assert testContainer == null;
        testContainer = testContainerCreator.get();
        testContainer.start();
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        terminateTestContainer();
    }

    @Override
    public void before(ExtensionContext context) throws Exception {
        terminateTestContainer();
        instantiateTestContainer();
    }
}
