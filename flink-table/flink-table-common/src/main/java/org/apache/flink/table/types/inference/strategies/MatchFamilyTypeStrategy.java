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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;

/** Type strategy that returns the given argument if it is of the same logical type family. */
@Internal
public final class MatchFamilyTypeStrategy implements TypeStrategy {

    private final int argumentPos;

    private final LogicalTypeFamily matchingTypeFamily;

    public MatchFamilyTypeStrategy(int argumentPos, LogicalTypeFamily matchingTypeFamily) {
        this.argumentPos = argumentPos;
        this.matchingTypeFamily = Preconditions.checkNotNull(matchingTypeFamily);
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final DataType argumentDataType = callContext.getArgumentDataTypes().get(argumentPos);
        if (hasFamily(argumentDataType.getLogicalType(), matchingTypeFamily)) {
            return Optional.of(argumentDataType);
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MatchFamilyTypeStrategy that = (MatchFamilyTypeStrategy) o;
        return argumentPos == that.argumentPos && matchingTypeFamily == that.matchingTypeFamily;
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentPos, matchingTypeFamily);
    }
}
