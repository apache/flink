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

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

/** Strategy for inferring a function call's intermediate result data type (i.e. state entry). */
@PublicEvolving
public interface StateTypeStrategy extends TypeStrategy {

    static StateTypeStrategy of(TypeStrategy typeStrategy) {
        return new DefaultStateTypeStrategy(typeStrategy, null);
    }

    static StateTypeStrategy of(TypeStrategy typeStrategy, @Nullable Duration timeToLive) {
        return new DefaultStateTypeStrategy(typeStrategy, timeToLive);
    }

    /**
     * The time-to-live (TTL) duration that automatically cleans up the state entry.
     *
     * <p>Returning {@code Optional.empty()} will fall back to default behavior. Returning a value
     * equal or greater than 0 means setting a custom TTL for this state entry and ignoring the
     * global defaults.
     */
    Optional<Duration> getTimeToLive(CallContext callContext);
}
