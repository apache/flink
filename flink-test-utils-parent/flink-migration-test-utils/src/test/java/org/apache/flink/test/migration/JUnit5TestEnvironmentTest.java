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

package org.apache.flink.test.migration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JUnit5TestEnvironment}. */
class JUnit5TestEnvironmentTest {

    @Test
    void testAfterAllClosesStoredValuesLikeJUnit() throws Exception {
        JUnit5TestEnvironment environment = new JUnit5TestEnvironment(ClassWithStore.class);
        environment.beforeAll();
        environment.afterAll();

        assertThat(ClassWithStore.EXTENSION.closed)
                .containsExactly("both", "autoCloseable", "closeableResource");
    }

    /** Test class whose static extension puts closeable values into two store namespaces. */
    static class ClassWithStore {
        @RegisterExtension static final StoringExtension EXTENSION = new StoringExtension();
    }

    private static final class StoringExtension implements BeforeAllCallback {

        private final List<String> closed = new ArrayList<>();

        @Override
        public void beforeAll(ExtensionContext context) {
            ExtensionContext.Store store = context.getStore(ExtensionContext.Namespace.GLOBAL);
            ExtensionContext.Store otherStore =
                    context.getStore(ExtensionContext.Namespace.create("other"));
            // Overwriting a key moves it to the end of the close order, as in JUnit's store.
            store.put("both", "placeholder");
            store.put(
                    "closeableResource",
                    (ExtensionContext.Store.CloseableResource)
                            () -> closed.add("closeableResource"));
            otherStore.put("autoCloseable", (AutoCloseable) () -> closed.add("autoCloseable"));
            store.put("both", new Both());
        }

        private final class Both
                implements ExtensionContext.Store.CloseableResource, AutoCloseable {
            @Override
            public void close() {
                closed.add("both");
            }
        }
    }
}
