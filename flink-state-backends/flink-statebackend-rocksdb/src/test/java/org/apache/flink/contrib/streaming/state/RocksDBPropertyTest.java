/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Validate RocksDB properties. */
class RocksDBPropertyTest {

    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    @Test
    void testRocksDBPropertiesValid() {
        RocksDB db = rocksDBExtension.getRocksDB();
        ColumnFamilyHandle handle = rocksDBExtension.getDefaultColumnFamily();

        for (RocksDBProperty property : RocksDBProperty.values()) {
            try {
                db.getLongProperty(handle, property.getRocksDBProperty());
            } catch (RocksDBException e) {
                throw new AssertionError(
                        String.format("Invalid RocksDB property %s", property.getRocksDBProperty()),
                        e);
            }
        }
    }
}
