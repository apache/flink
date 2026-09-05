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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.GeographyData;

import java.io.IOException;

/** Serializer snapshot for {@link GeographyTypeSerializer}. */
@Internal
public final class GeographyTypeSerializerSnapshot
        implements TypeSerializerSnapshot<GeographyData> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {}

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {}

    @Override
    public TypeSerializer<GeographyData> restoreSerializer() {
        return GeographyTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<GeographyData> resolveSchemaCompatibility(
            TypeSerializerSnapshot<GeographyData> oldSerializerSnapshot) {
        if (oldSerializerSnapshot instanceof GeographyTypeSerializerSnapshot) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
        return TypeSerializerSchemaCompatibility.incompatible();
    }
}
