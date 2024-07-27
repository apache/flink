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

package org.apache.flink.table.file.testutils.catalog;

import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

/** Base class for test filesystem catalog. */
public abstract class TestFileSystemCatalogTestBase extends AbstractTestBase {

    protected static final String TEST_CATALOG = "test_catalog";
    protected static final String TEST_DEFAULT_DATABASE = "test_db";
    protected static final String NONE_EXIST_DATABASE = "none_exist_database";

    protected TestFileSystemCatalog catalog;

    @TempDir File tempFile;

    @BeforeEach
    void before() {
        File testDb = new File(tempFile, TEST_DEFAULT_DATABASE);
        testDb.mkdir();

        String catalogPathStr = tempFile.getAbsolutePath();
        catalog = new TestFileSystemCatalog(catalogPathStr, TEST_CATALOG, TEST_DEFAULT_DATABASE);
        catalog.open();
    }

    @AfterEach
    void close() {
        if (catalog != null) {
            catalog.close();
        }
    }
}
