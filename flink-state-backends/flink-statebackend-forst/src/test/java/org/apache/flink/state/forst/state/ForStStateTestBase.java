/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.state;

import org.apache.flink.configuration.ConfigConstants;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

/** The base class for ForSt State tests. */
public class ForStStateTestBase {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    protected RocksDB db;

    @Before
    public void setUp() throws Exception {
        db = RocksDB.open(folder.newFolder().getAbsolutePath());
    }

    @After
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();
        }
    }

    protected ColumnFamilyHandle createColumnFamilyHandle(String columnFamilyName)
            throws Exception {
        byte[] nameBytes = columnFamilyName.getBytes(ConfigConstants.DEFAULT_CHARSET);
        ColumnFamilyDescriptor columnFamilyDescriptor =
                new ColumnFamilyDescriptor(nameBytes, new ColumnFamilyOptions());
        return db.createColumnFamily(columnFamilyDescriptor);
    }
}
