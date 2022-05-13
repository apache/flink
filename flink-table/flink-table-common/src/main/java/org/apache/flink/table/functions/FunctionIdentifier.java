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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Identifies a system function with function name or a catalog function with a fully qualified
 * identifier. Function catalog is responsible for resolving an identifier to a function.
 */
@PublicEvolving
public final class FunctionIdentifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private final @Nullable ObjectIdentifier objectIdentifier;

    private final @Nullable String functionName;

    public static FunctionIdentifier of(ObjectIdentifier oi) {
        return new FunctionIdentifier(oi);
    }

    public static FunctionIdentifier of(String functionName) {
        return new FunctionIdentifier(functionName);
    }

    private FunctionIdentifier(ObjectIdentifier objectIdentifier) {
        checkNotNull(objectIdentifier, "Object identifier cannot be null");
        this.objectIdentifier = objectIdentifier;
        this.functionName = null;
    }

    private FunctionIdentifier(String functionName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(functionName),
                "function name cannot be null or empty string");
        this.functionName = functionName;
        this.objectIdentifier = null;
    }

    /** Normalize a function name. */
    public static String normalizeName(String name) {
        return name.toLowerCase();
    }

    /** Normalize an object identifier by only normalizing the function name. */
    public static ObjectIdentifier normalizeObjectIdentifier(ObjectIdentifier oi) {
        return ObjectIdentifier.of(
                oi.getCatalogName(), oi.getDatabaseName(), normalizeName(oi.getObjectName()));
    }

    public Optional<ObjectIdentifier> getIdentifier() {
        return Optional.ofNullable(objectIdentifier);
    }

    public Optional<String> getSimpleName() {
        return Optional.ofNullable(functionName);
    }

    /** List of the component names of this function identifier. */
    public List<String> toList() {
        if (objectIdentifier != null) {
            return objectIdentifier.toList();
        } else if (functionName != null) {
            return Collections.singletonList(functionName);
        } else {
            throw new IllegalStateException(
                    "functionName and objectIdentifier are both null which should never happen.");
        }
    }

    /** Returns a string that summarizes this instance for printing to a console or log. */
    public String asSummaryString() {
        if (objectIdentifier != null) {
            return String.join(
                    ".",
                    objectIdentifier.getCatalogName(),
                    objectIdentifier.getDatabaseName(),
                    objectIdentifier.getObjectName());
        } else {
            return functionName;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionIdentifier that = (FunctionIdentifier) o;

        return Objects.equals(objectIdentifier, that.objectIdentifier)
                && Objects.equals(functionName, that.functionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectIdentifier, functionName);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
