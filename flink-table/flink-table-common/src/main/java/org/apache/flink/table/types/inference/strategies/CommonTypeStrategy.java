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
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Type strategy that returns a common, least restrictive type of selected arguments. */
@Internal
public final class CommonTypeStrategy implements TypeStrategy {

    private final ArgumentCount argumentRange;

    public CommonTypeStrategy() {
        this.argumentRange = ConstantArgumentCount.any();
    }

    public CommonTypeStrategy(ArgumentCount argumentRange) {
        this.argumentRange = argumentRange;
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        int minCount = argumentRange.getMinCount().orElse(0);
        int maxCount =
                argumentRange.getMaxCount().orElse(callContext.getArgumentDataTypes().size() - 1);
        Preconditions.checkArgument(minCount <= maxCount);

        final List<LogicalType> selectedActualTypes =
                callContext.getArgumentDataTypes().subList(minCount, maxCount + 1).stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());
        return LogicalTypeMerging.findCommonType(selectedActualTypes)
                .map(TypeConversions::fromLogicalToDataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CommonTypeStrategy)) {
            return false;
        }
        CommonTypeStrategy that = (CommonTypeStrategy) o;
        return argumentRange.equals(that.argumentRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentRange);
    }
}
