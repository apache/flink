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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.GlobalCommitterTransform;
import org.apache.flink.util.function.SerializableSupplier;

/** This utility class provides building blocks for custom topologies. */
@Experimental
public class StandardSinkTopologies {

    public static final String GLOBAL_COMMITTER_TRANSFORMATION_NAME = "Global Committer";

    private StandardSinkTopologies() {}

    /**
     * Adds a global committer to the pipeline that runs as final operator with a parallelism of
     * one.
     */
    public static <CommT> void addGlobalCommitter(
            DataStream<CommittableMessage<CommT>> committables,
            SerializableSupplier<Committer<CommT>> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializer) {
        committables
                .getExecutionEnvironment()
                .addOperator(
                        new GlobalCommitterTransform<>(
                                committables, committerFactory, committableSerializer));
    }
}
