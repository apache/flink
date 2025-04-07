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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** A helper class that wraps a {@link TypeStrategy} into a {@link StateTypeStrategy}. */
@Internal
class DefaultStateTypeStrategy implements StateTypeStrategy {

    private final TypeStrategy typeStrategy;
    private final @Nullable Duration timeToLive;

    DefaultStateTypeStrategy(TypeStrategy typeStrategy, @Nullable Duration timeToLive) {
        this.typeStrategy =
                Preconditions.checkNotNull(typeStrategy, "Type strategy must not be null.");
        this.timeToLive = timeToLive;
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        return typeStrategy.inferType(callContext);
    }

    @Override
    public Optional<Duration> getTimeToLive(CallContext callContext) {
        return Optional.ofNullable(timeToLive);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DefaultStateTypeStrategy that = (DefaultStateTypeStrategy) o;
        return Objects.equals(typeStrategy, that.typeStrategy)
                && Objects.equals(timeToLive, that.timeToLive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeStrategy, timeToLive);
    }
}
