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

package org.apache.flink.table.gateway.api.results;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Info to describe the function. It is not equivalent to the {@link FunctionDefinition} that needs
 * to load the implementation, which may require to download the jar from the remote to the local
 * machine and load the classes. Comparing to the {@link FunctionDefinition}, the {@link
 * FunctionInfo} return the available information in the current state, which is much lighter.
 */
@PublicEvolving
public class FunctionInfo {

    /** Identifier of the function. */
    private final FunctionIdentifier identifier;
    /** Kind of the function. If the value is null, it means kind of the function is unresolved. */
    private final @Nullable FunctionKind kind;

    public FunctionInfo(FunctionIdentifier identifier) {
        this(identifier, null);
    }

    public FunctionInfo(FunctionIdentifier identifier, @Nullable FunctionKind kind) {
        this.identifier = identifier;
        this.kind = kind;
    }

    public FunctionIdentifier getIdentifier() {
        return identifier;
    }

    public Optional<FunctionKind> getKind() {
        return Optional.ofNullable(kind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FunctionInfo)) {
            return false;
        }
        FunctionInfo that = (FunctionInfo) o;
        return Objects.equals(identifier, that.identifier) && kind == that.kind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, kind);
    }

    @Override
    public String toString() {
        return "FunctionInfo{identifier=" + identifier + ", kind=" + kind + '}';
    }
}
