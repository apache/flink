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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;

/** Wrap the TypeSerializerSnapshot restored from {@link TypeSerializerSnapshot} */
public class TtlAwareSerializerSnapshotWrapper<T> {

    private final TypeSerializerSnapshot<T> typeSerializerSnapshot;

    public TtlAwareSerializerSnapshotWrapper(TypeSerializerSnapshot<T> typeSerializerSnapshot) {
        this.typeSerializerSnapshot = typeSerializerSnapshot;
    }

    public TypeSerializerSnapshot<T> getTtlAwareSerializerSnapshot() {
        if (typeSerializerSnapshot instanceof ListSerializerSnapshot) {
            return wrapListSerializerSnapshot();
        } else if (typeSerializerSnapshot instanceof MapSerializerSnapshot) {
            return wrapMapSerializerSnapshot();
        } else {
            return wrapValueSerializerSnapshot();
        }
    }

    private TypeSerializerSnapshot<T> wrapValueSerializerSnapshot() {
        return typeSerializerSnapshot instanceof TtlAwareSerializerSnapshot
                ? typeSerializerSnapshot
                : new TtlAwareSerializerSnapshot<>(typeSerializerSnapshot);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializerSnapshot<T> wrapListSerializerSnapshot() {
        ListSerializerSnapshot listSerializerSnapshot =
                (ListSerializerSnapshot) typeSerializerSnapshot;
        if (!(listSerializerSnapshot.getElementSerializerSnapshot()
                instanceof TtlAwareSerializerSnapshot)) {
            CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
                    listSerializerSnapshot,
                    new TtlAwareSerializerSnapshot<>(
                            listSerializerSnapshot.getElementSerializerSnapshot()));
        }
        return listSerializerSnapshot;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializerSnapshot<T> wrapMapSerializerSnapshot() {
        MapSerializerSnapshot mapSerializerSnapshot =
                (MapSerializerSnapshot) typeSerializerSnapshot;
        if (!(mapSerializerSnapshot.getValueSerializerSnapshot()
                instanceof TtlAwareSerializerSnapshot)) {
            CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
                    mapSerializerSnapshot,
                    mapSerializerSnapshot.getKeySerializerSnapshot(),
                    new TtlAwareSerializerSnapshot<>(
                            mapSerializerSnapshot.getValueSerializerSnapshot()));
        }
        return mapSerializerSnapshot;
    }
}
