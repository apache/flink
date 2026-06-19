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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/** Type strategy that returns the n-th input argument, mapping it with the provided function. */
@Internal
public final class ArgumentMappingTypeStrategy implements TypeStrategy {

    private final int pos;
    private final Function<DataType, Optional<DataType>> mapper;

    public ArgumentMappingTypeStrategy(int pos, Function<DataType, Optional<DataType>> mapper) {
        Preconditions.checkArgument(pos >= 0);
        Preconditions.checkNotNull(mapper);
        this.pos = pos;
        this.mapper = mapper;
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (pos >= argumentDataTypes.size()) {
            return Optional.empty();
        }
        return mapper.apply(argumentDataTypes.get(pos));
    }
}
