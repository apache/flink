/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb.sstmerge;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Lookup helper for RocksDB column families by their name. */
class ColumnFamilyLookup {
    private final Map<Key, ColumnFamilyHandle> map;

    public ColumnFamilyLookup() {
        map = new ConcurrentHashMap<>();
    }

    @Nullable
    public ColumnFamilyHandle get(byte[] name) {
        return map.get(new Key(name));
    }

    public void add(ColumnFamilyHandle handle) {
        try {
            map.put(new Key(handle.getName()), handle);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Key {
        private final byte[] payload;

        private Key(byte[] payload) {
            this.payload = checkNotNull(payload);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Arrays.equals(payload, key.payload);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(payload);
        }
    }
}
