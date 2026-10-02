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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;

/** Utilities for checking serializer schema compatibility during nullability widening. */
@Internal
public final class SerializerSchemaCompatibilityUtils {

    /**
     * Returns true when the given serializer is schema-compatible-as-is with the previous
     * serializer after nullability widening (NOT NULL → NULL).
     */
    public static boolean isSerializerCompatibleAfterNullabilityWidening(
            TypeSerializer<?> serializer, TypeSerializer<?> previousSerializer) {
        if (serializer.equals(previousSerializer)) {
            return true;
        }

        TypeSerializerSchemaCompatibility<?> compatibility =
                resolveSchemaCompatibility(serializer, previousSerializer);
        return compatibility.isCompatibleAsIs();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TypeSerializerSchemaCompatibility<?> resolveSchemaCompatibility(
            TypeSerializer<?> serializer, TypeSerializer<?> previousSerializer) {
        TypeSerializerSnapshot previousSnapshot = previousSerializer.snapshotConfiguration();
        return serializer.snapshotConfiguration().resolveSchemaCompatibility(previousSnapshot);
    }

    private SerializerSchemaCompatibilityUtils() {}
}
