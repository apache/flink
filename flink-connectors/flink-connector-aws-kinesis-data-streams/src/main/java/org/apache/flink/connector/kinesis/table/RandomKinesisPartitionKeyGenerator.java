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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;

import java.util.UUID;

/**
 * A {@link PartitionKeyGenerator} that maps an arbitrary input {@code element} to a random
 * partition ID.
 *
 * @param <T> The input element type.
 */
@PublicEvolving
public final class RandomKinesisPartitionKeyGenerator<T> implements PartitionKeyGenerator<T> {
    @Override
    public String apply(T element) {
        return UUID.randomUUID().toString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RandomKinesisPartitionKeyGenerator;
    }

    @Override
    public int hashCode() {
        return RandomKinesisPartitionKeyGenerator.class.hashCode();
    }
}
