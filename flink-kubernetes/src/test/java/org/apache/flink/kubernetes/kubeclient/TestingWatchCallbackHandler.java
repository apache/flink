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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient.WatchCallbackHandler;

import java.util.List;
import java.util.function.Consumer;

/** Mock {@link WatchCallbackHandler} for testing. */
public class TestingWatchCallbackHandler<T> implements WatchCallbackHandler<T> {
    private final Consumer<List<T>> onAddedConsumer;

    private final Consumer<List<T>> onModifiedConsumer;

    private final Consumer<List<T>> onDeletedConsumer;

    private final Consumer<List<T>> onErrorConsumer;

    private final Consumer<Throwable> handleErrorConsumer;

    private TestingWatchCallbackHandler(
            Consumer<List<T>> onAddedConsumer,
            Consumer<List<T>> onModifiedConsumer,
            Consumer<List<T>> onDeletedConsumer,
            Consumer<List<T>> onErrorConsumer,
            Consumer<Throwable> handleErrorConsumer) {
        this.onAddedConsumer = onAddedConsumer;
        this.onModifiedConsumer = onModifiedConsumer;
        this.onDeletedConsumer = onDeletedConsumer;
        this.onErrorConsumer = onErrorConsumer;
        this.handleErrorConsumer = handleErrorConsumer;
    }

    @Override
    public void onAdded(List<T> resources) {
        onAddedConsumer.accept(resources);
    }

    @Override
    public void onModified(List<T> resources) {
        onModifiedConsumer.accept(resources);
    }

    @Override
    public void onDeleted(List<T> resources) {
        onDeletedConsumer.accept(resources);
    }

    @Override
    public void onError(List<T> resources) {
        onErrorConsumer.accept(resources);
    }

    @Override
    public void handleError(Throwable throwable) {
        handleErrorConsumer.accept(throwable);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder for {@link TestingWatchCallbackHandler}. */
    public static class Builder<T> {
        private Consumer<List<T>> onAddedConsumer = (ignore) -> {};

        private Consumer<List<T>> onModifiedConsumer = (ignore) -> {};

        private Consumer<List<T>> onDeletedConsumer = (ignore) -> {};

        private Consumer<List<T>> onErrorConsumer = (ignore) -> {};

        private Consumer<Throwable> handleErrorConsumer = (ignore) -> {};

        private Builder() {}

        public Builder<T> setOnAddedConsumer(Consumer<List<T>> onAddedConsumer) {
            this.onAddedConsumer = onAddedConsumer;
            return this;
        }

        public Builder<T> setOnModifiedConsumer(Consumer<List<T>> onModifiedConsumer) {
            this.onModifiedConsumer = onModifiedConsumer;
            return this;
        }

        public Builder<T> setOnDeletedConsumer(Consumer<List<T>> onDeletedConsumer) {
            this.onDeletedConsumer = onDeletedConsumer;
            return this;
        }

        public Builder<T> setOnErrorConsumer(Consumer<List<T>> onErrorConsumer) {
            this.onErrorConsumer = onErrorConsumer;
            return this;
        }

        public Builder<T> setHandleErrorConsumer(Consumer<Throwable> handleErrorConsumer) {
            this.handleErrorConsumer = handleErrorConsumer;
            return this;
        }

        public TestingWatchCallbackHandler<T> build() {
            return new TestingWatchCallbackHandler<>(
                    onAddedConsumer,
                    onModifiedConsumer,
                    onDeletedConsumer,
                    onErrorConsumer,
                    handleErrorConsumer);
        }
    }
}
