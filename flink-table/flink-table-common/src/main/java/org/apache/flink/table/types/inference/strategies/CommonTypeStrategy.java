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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Type strategy that returns a common, least restrictive type of all arguments. */
@Internal
public final class CommonTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<LogicalType> actualTypes =
                callContext.getArgumentDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());
        return LogicalTypeMerging.findCommonType(actualTypes)
                .map(TypeConversions::fromLogicalToDataType);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof CommonTypeStrategy;
    }

    @Override
    public int hashCode() {
        return CommonTypeStrategy.class.hashCode();
    }
}
