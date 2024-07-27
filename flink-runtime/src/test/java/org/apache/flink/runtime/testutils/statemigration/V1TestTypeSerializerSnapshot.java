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

package org.apache.flink.runtime.testutils.statemigration;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.testutils.statemigration.TestType.IncompatibleTestTypeSerializer.IncompatibleTestTypeSerializerSnapshot;
import org.apache.flink.runtime.testutils.statemigration.TestType.ReconfigurationRequiringTestTypeSerializer.ReconfigurationRequiringTestTypeSerializerSnapshot;

import java.io.IOException;

/** Snapshot class for {@link TestType.V1TestTypeSerializer}. */
public class V1TestTypeSerializerSnapshot implements TypeSerializerSnapshot<TestType> {

    @Override
    public int getCurrentVersion() {
        return 1;
    }

    @Override
    public TypeSerializerSchemaCompatibility<TestType> resolveSchemaCompatibility(
            TypeSerializerSnapshot<TestType> oldSerializerSnapshot) {
        if (oldSerializerSnapshot instanceof V1TestTypeSerializerSnapshot) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        } else if (
        // old ReconfigurationRequiringTestTypeSerializerSnapshot cannot be compatible with
        // any new  TypeSerializerSnapshots
        oldSerializerSnapshot instanceof ReconfigurationRequiringTestTypeSerializerSnapshot
                // migrating from V2 -> V1 is not supported
                || oldSerializerSnapshot instanceof V2TestTypeSerializerSnapshot
                // IncompatibleTestTypeSerializerSnapshot cannot be compatible with any
                // TypeSerializerSnapshots
                || oldSerializerSnapshot instanceof IncompatibleTestTypeSerializerSnapshot) {
            return TypeSerializerSchemaCompatibility.incompatible();
        } else {
            throw new IllegalStateException("Unknown serializer class for TestType.");
        }
    }

    @Override
    public TypeSerializer<TestType> restoreSerializer() {
        return new TestType.V1TestTypeSerializer();
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {}

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {}
}
