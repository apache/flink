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

/** Type strategy that returns the n-th input argument. */
@Internal
public final class UseArgumentTypeStrategy implements TypeStrategy {

    private final int pos;

    public UseArgumentTypeStrategy(int pos) {
        Preconditions.checkArgument(pos >= 0);
        this.pos = pos;
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (pos >= argumentDataTypes.size()) {
            return Optional.empty();
        }
        return Optional.of(argumentDataTypes.get(pos));
    }
}
