/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.util;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.runtime.SqlFunctions;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * The whole class was copied from Calcite because of CALCITE-7716. Should be removed after upgrade
 * to 1.43.0
 */
public class UuidValue implements Comparable<UuidValue> {

    private final UUID uuid;

    public UuidValue(UUID uuid) {
        this.uuid = requireNonNull(uuid, "uuid");
    }

    /**
     * Creates a UuidValue from any of the spellings accepted by {@link SqlFunctions#stringToUuid},
     * in which hyphens are optional group separators; called from generated code.
     */
    public static UuidValue fromString(String s) {
        return new UuidValue(SqlFunctions.stringToUuid(s));
    }

    /** Returns the wrapped {@link UUID}. */
    public UUID uuid() {
        return uuid;
    }

    /** Returns the least significant 64 bits of this UUID. */
    public long getLeastSignificantBits() {
        return uuid.getLeastSignificantBits();
    }

    /** Returns the most significant 64 bits of this UUID. */
    public long getMostSignificantBits() {
        return uuid.getMostSignificantBits();
    }

    /**
     * Compares two UUIDs as unsigned 128-bit values, or, if {@link
     * CalciteSystemProperty#UUID_UNSIGNED_COMPARISON} is off, using {@link UUID#compareTo}.
     */
    @Override
    public int compareTo(UuidValue that) {
        /*
        FLINK MODIFICATION BEGIN
        if (!UNSIGNED_COMPARISON) {
            return uuid.compareTo(that.uuid);
        }
        FLINK MODIFICATION END
         */
        final int c =
                Long.compareUnsigned(
                        uuid.getMostSignificantBits(), that.uuid.getMostSignificantBits());
        if (c != 0) {
            return c;
        }
        return Long.compareUnsigned(
                uuid.getLeastSignificantBits(), that.uuid.getLeastSignificantBits());
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        return this == obj || obj instanceof UuidValue && uuid.equals(((UuidValue) obj).uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public String toString() {
        return uuid.toString();
    }
}
