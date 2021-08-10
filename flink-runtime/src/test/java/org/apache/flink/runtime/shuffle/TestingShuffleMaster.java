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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkState;

/** A {@link ShuffleMaster} implementation for tests. */
public class TestingShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {

    boolean autoCompleteRegistration = true;

    boolean throwExceptionalOnRegistration = false;

    private final Queue<Tuple2<PartitionDescriptor, ProducerDescriptor>>
            pendingPartitionRegistrations = new ArrayBlockingQueue<>(4);

    private final Queue<CompletableFuture<ShuffleDescriptor>>
            pendingPartitionRegistrationResponses = new ArrayBlockingQueue<>(4);

    private final Queue<ShuffleDescriptor> externallyReleasedPartitions =
            new ArrayBlockingQueue<>(4);

    @Override
    public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {
        if (throwExceptionalOnRegistration) {
            throw new RuntimeException("Forced partition registration failure");
        } else if (autoCompleteRegistration) {
            return CompletableFuture.completedFuture(
                    createShuffleDescriptor(partitionDescriptor, producerDescriptor));
        } else {
            CompletableFuture<ShuffleDescriptor> response = new CompletableFuture<>();
            pendingPartitionRegistrations.add(
                    new Tuple2<>(partitionDescriptor, producerDescriptor));
            pendingPartitionRegistrationResponses.add(response);
            return response;
        }
    }

    private ShuffleDescriptor createShuffleDescriptor(
            PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {

        ResultPartitionID resultPartitionId =
                new ResultPartitionID(
                        partitionDescriptor.getPartitionId(),
                        producerDescriptor.getProducerExecutionId());
        return new TestingShuffleDescriptor(
                resultPartitionId, producerDescriptor.getProducerLocation());
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
        externallyReleasedPartitions.add(shuffleDescriptor);
    }

    public Queue<ShuffleDescriptor> getExternallyReleasedPartitions() {
        return externallyReleasedPartitions;
    }

    public void setAutoCompleteRegistration(boolean autoCompleteRegistration) {
        this.autoCompleteRegistration = autoCompleteRegistration;
    }

    public void setThrowExceptionalOnRegistration(boolean throwExceptionalOnRegistration) {
        this.throwExceptionalOnRegistration = throwExceptionalOnRegistration;
    }

    public void completeAllPendingRegistrations() {
        processPendingRegistrations(
                (response, tuple) ->
                        response.complete(createShuffleDescriptor(tuple.f0, tuple.f1)));
    }

    public void failAllPendingRegistrations() {
        processPendingRegistrations(
                (response, ignore) ->
                        response.completeExceptionally(
                                new Exception("Forced partition registration failure")));
    }

    private void processPendingRegistrations(
            BiConsumer<
                            CompletableFuture<ShuffleDescriptor>,
                            Tuple2<PartitionDescriptor, ProducerDescriptor>>
                    processor) {

        checkState(
                pendingPartitionRegistrationResponses.size()
                        == pendingPartitionRegistrations.size());

        Tuple2<PartitionDescriptor, ProducerDescriptor> tuple;
        while ((tuple = pendingPartitionRegistrations.poll()) != null) {
            processor.accept(pendingPartitionRegistrationResponses.poll(), tuple);
        }
    }

    private static class TestingShuffleDescriptor implements ShuffleDescriptor {

        private final ResultPartitionID resultPartitionId;

        private final ResourceID location;

        TestingShuffleDescriptor(ResultPartitionID resultPartitionId, ResourceID location) {
            this.resultPartitionId = resultPartitionId;
            this.location = location;
        }

        @Override
        public ResultPartitionID getResultPartitionID() {
            return resultPartitionId;
        }

        @Override
        public Optional<ResourceID> storesLocalResourcesOn() {
            return Optional.of(location);
        }
    }
}
