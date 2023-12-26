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

package org.apache.flink.runtime.io.network.partition;

import java.io.IOException;
import java.util.Optional;

/** {@link ResultPartitionProvider} implementation for testing purposes. */
public class TestingResultPartitionProvider implements ResultPartitionProvider {
    private final CreateSubpartitionView createSubpartitionViewFunction;
    private final CreateSubpartitionViewOrRegisterListener
            createSubpartitionViewOrRegisterListenerFunction;
    private final ReleasePartitionRequestListener releasePartitionRequestListenerConsumer;

    public TestingResultPartitionProvider(
            CreateSubpartitionView createSubpartitionViewFunction,
            CreateSubpartitionViewOrRegisterListener
                    createSubpartitionViewOrRegisterListenerFunction,
            ReleasePartitionRequestListener releasePartitionRequestListenerConsumer) {
        this.createSubpartitionViewFunction = createSubpartitionViewFunction;
        this.createSubpartitionViewOrRegisterListenerFunction =
                createSubpartitionViewOrRegisterListenerFunction;
        this.releasePartitionRequestListenerConsumer = releasePartitionRequestListenerConsumer;
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            int index,
            BufferAvailabilityListener availabilityListener)
            throws IOException {
        return createSubpartitionViewFunction.createSubpartitionView(
                partitionId, index, availabilityListener);
    }

    @Override
    public Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
            ResultPartitionID partitionId,
            int index,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestListener notifier)
            throws IOException {
        return createSubpartitionViewOrRegisterListenerFunction
                .createSubpartitionViewOrRegisterListener(
                        partitionId, index, availabilityListener, notifier);
    }

    @Override
    public void releasePartitionRequestListener(PartitionRequestListener notifier) {
        releasePartitionRequestListenerConsumer.releasePartitionRequestListener(notifier);
    }

    public static TestingResultPartitionProviderBuilder newBuilder() {
        return new TestingResultPartitionProviderBuilder();
    }

    /** Factory for {@link TestingResultPartitionProvider}. */
    public static class TestingResultPartitionProviderBuilder {
        private CreateSubpartitionView createSubpartitionViewFunction =
                (resultPartitionID, index, availabilityListener) -> null;
        private CreateSubpartitionViewOrRegisterListener
                createSubpartitionViewOrRegisterListenerFunction =
                        (partitionId, index, availabilityListener, partitionRequestListener) ->
                                Optional.empty();
        private ReleasePartitionRequestListener releasePartitionRequestListenerConsumer =
                listener -> {};

        public TestingResultPartitionProviderBuilder setCreateSubpartitionViewFunction(
                CreateSubpartitionView createSubpartitionViewFunction) {
            this.createSubpartitionViewFunction = createSubpartitionViewFunction;
            return this;
        }

        public TestingResultPartitionProviderBuilder setCreateSubpartitionViewOrNotifyFunction(
                CreateSubpartitionViewOrRegisterListener
                        createSubpartitionViewOrRegisterListenerFunction) {
            this.createSubpartitionViewOrRegisterListenerFunction =
                    createSubpartitionViewOrRegisterListenerFunction;
            return this;
        }

        public TestingResultPartitionProviderBuilder setReleasePartitionRequestListenerConsumer(
                ReleasePartitionRequestListener releasePartitionRequestListenerConsumer) {
            this.releasePartitionRequestListenerConsumer = releasePartitionRequestListenerConsumer;
            return this;
        }

        public TestingResultPartitionProvider build() {
            return new TestingResultPartitionProvider(
                    createSubpartitionViewFunction,
                    createSubpartitionViewOrRegisterListenerFunction,
                    releasePartitionRequestListenerConsumer);
        }
    }

    /** Testing interface for createSubpartitionView. */
    public interface CreateSubpartitionView {
        ResultSubpartitionView createSubpartitionView(
                ResultPartitionID partitionId,
                int index,
                BufferAvailabilityListener availabilityListener)
                throws IOException;
    }

    /** Testing interface for createSubpartitionViewOrRegisterListener. */
    public interface CreateSubpartitionViewOrRegisterListener {
        Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
                ResultPartitionID partitionId,
                int index,
                BufferAvailabilityListener availabilityListener,
                PartitionRequestListener partitionRequestListener)
                throws IOException;
    }

    /** Testing interface for releasePartitionRequestListener. */
    public interface ReleasePartitionRequestListener {
        void releasePartitionRequestListener(PartitionRequestListener listener);
    }
}
