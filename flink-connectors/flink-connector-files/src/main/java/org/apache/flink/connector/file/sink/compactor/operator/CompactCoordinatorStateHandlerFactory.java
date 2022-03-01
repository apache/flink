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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.types.Either;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;

/** Factory for {@link CompactCoordinatorStateHandler}. */
@Internal
public class CompactCoordinatorStateHandlerFactory
        extends AbstractStreamOperatorFactory<
                Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>>
        implements OneInputStreamOperatorFactory<
                CommittableMessage<FileSinkCommittable>,
                Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>> {

    private final SerializableSupplierWithException<
                    SimpleVersionedSerializer<FileSinkCommittable>, IOException>
            committableSerializerSupplier;

    public CompactCoordinatorStateHandlerFactory(
            SerializableSupplierWithException<
                            SimpleVersionedSerializer<FileSinkCommittable>, IOException>
                    committableSerializerSupplier) {
        this.committableSerializerSupplier = committableSerializerSupplier;
    }

    @Override
    public <
                    T extends
                            StreamOperator<
                                            Either<
                                                    CommittableMessage<FileSinkCommittable>,
                                                    CompactorRequest>>>
            T createStreamOperator(
                    StreamOperatorParameters<
                                    Either<
                                            CommittableMessage<FileSinkCommittable>,
                                            CompactorRequest>>
                            parameters) {
        try {
            final CompactCoordinatorStateHandler handler =
                    new CompactCoordinatorStateHandler(committableSerializerSupplier.get());
            handler.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) handler;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create commit operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CompactCoordinator.class;
    }
}
