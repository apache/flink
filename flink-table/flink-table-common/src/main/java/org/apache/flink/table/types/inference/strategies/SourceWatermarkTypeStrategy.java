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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;

/** Type strategy specific for source watermarks that depend on the output type. */
@Internal
class SourceWatermarkTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final DataType timestampDataType =
                callContext
                        .getOutputDataType()
                        .filter(dt -> hasFamily(dt.getLogicalType(), LogicalTypeFamily.TIMESTAMP))
                        .orElse(DataTypes.TIMESTAMP_LTZ(3));
        return Optional.of(timestampDataType);
    }
}
