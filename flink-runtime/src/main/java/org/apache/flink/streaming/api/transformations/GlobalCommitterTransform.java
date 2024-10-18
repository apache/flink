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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * Transformation for global committer. Only used to fetch if the pipeline is streaming or batch
 * with the respective {@link
 * org.apache.flink.streaming.runtime.translators.GlobalCommitterTransformationTranslator}.
 *
 * @param <CommT>
 */
@Internal
public class GlobalCommitterTransform<CommT> extends TransformationWithLineage<Void> {

    private final DataStream<CommittableMessage<CommT>> inputStream;
    private final SerializableSupplier<Committer<CommT>> committerFactory;
    private final SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializer;

    public GlobalCommitterTransform(
            DataStream<CommittableMessage<CommT>> inputStream,
            SerializableSupplier<Committer<CommT>> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializer) {
        super(StandardSinkTopologies.GLOBAL_COMMITTER_TRANSFORMATION_NAME, Types.VOID, 1, true);
        this.inputStream = inputStream;
        this.committerFactory = committerFactory;
        this.committableSerializer = committableSerializer;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {}

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        final List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        result.addAll(inputStream.getTransformation().getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(inputStream.getTransformation());
    }

    public DataStream<CommittableMessage<CommT>> getInputStream() {
        return inputStream;
    }

    public SerializableSupplier<Committer<CommT>> getCommitterFactory() {
        return committerFactory;
    }

    public SerializableSupplier<SimpleVersionedSerializer<CommT>> getCommittableSerializer() {
        return committableSerializer;
    }
}
