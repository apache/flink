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

package org.apache.flink.process.impl.context;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Mock implementation of {@link AllKeysContext} for testing. */
public class TestingAllKeysContext implements AllKeysContext {
    private final Consumer<Object> onKeySelectedConsumer;

    private final Supplier<Iterator<Object>> getAllKeysIterSupplier;

    private TestingAllKeysContext(
            Consumer<Object> onKeySelectedConsumer,
            Supplier<Iterator<Object>> getAllKeysIterSupplier) {
        this.onKeySelectedConsumer = onKeySelectedConsumer;
        this.getAllKeysIterSupplier = getAllKeysIterSupplier;
    }

    @Override
    public void onKeySelected(Object newKey) {
        onKeySelectedConsumer.accept(newKey);
    }

    @Override
    public Iterator<Object> getAllKeysIter() {
        return getAllKeysIterSupplier.get();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link TestingAllKeysContext}. */
    public static class Builder {
        private Consumer<Object> onKeySelectedConsumer = (ignore) -> {};

        private Supplier<Iterator<Object>> getAllKeysIterSupplier = () -> null;

        public Builder setOnKeySelectedConsumer(Consumer<Object> onKeySelectedConsumer) {
            this.onKeySelectedConsumer = onKeySelectedConsumer;
            return this;
        }

        public Builder setGetAllKeysIterSupplier(
                Supplier<Iterator<Object>> getAllKeysIterSupplier) {
            this.getAllKeysIterSupplier = getAllKeysIterSupplier;
            return this;
        }

        public TestingAllKeysContext build() {
            return new TestingAllKeysContext(onKeySelectedConsumer, getAllKeysIterSupplier);
        }
    }
}
