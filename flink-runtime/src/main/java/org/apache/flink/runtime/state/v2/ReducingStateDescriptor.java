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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;

/**
 * This class is deprecated and only a placeholder for compatibility for EXISTING PRs. This is never
 * released before, and will be safely removed before 2.0 release.
 */
@Deprecated
public class ReducingStateDescriptor<T>
        extends org.apache.flink.api.common.state.v2.ReducingStateDescriptor<T> {

    public ReducingStateDescriptor(
            @Nonnull String name,
            @Nonnull ReduceFunction<T> reduceFunction,
            @Nonnull TypeInformation<T> typeInfo) {
        super(name, reduceFunction, typeInfo);
    }

    public ReducingStateDescriptor(
            @Nonnull String stateId,
            @Nonnull ReduceFunction<T> reduceFunction,
            @Nonnull TypeSerializer<T> serializer) {
        super(stateId, reduceFunction, serializer);
    }
}
