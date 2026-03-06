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

package org.apache.flink.table.catalog;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ObjectPath}. */
class ObjectPathTest {

    @Test
    void testCompareToWithDifferentDatabaseName() {
        ObjectPath path1 = new ObjectPath("aDatabase", "table1");
        ObjectPath path2 = new ObjectPath("bDatabase", "table1");

        assertThat(path1.compareTo(path2)).isLessThan(0);
        assertThat(path2.compareTo(path1)).isGreaterThan(0);
    }

    @Test
    void testCompareToWithSameDatabaseDifferentObjectName() {
        ObjectPath path1 = new ObjectPath("myDb", "aTable");
        ObjectPath path2 = new ObjectPath("myDb", "bTable");

        assertThat(path1.compareTo(path2)).isLessThan(0);
        assertThat(path2.compareTo(path1)).isGreaterThan(0);
    }

    @Test
    void testCompareToWithEqualPaths() {
        ObjectPath path1 = new ObjectPath("myDb", "myTable");
        ObjectPath path2 = new ObjectPath("myDb", "myTable");

        assertThat(path1.compareTo(path2)).isEqualTo(0);
    }

    @Test
    void testCompareToWithSelf() {
        ObjectPath path = new ObjectPath("myDb", "myTable");

        assertThat(path.compareTo(path)).isEqualTo(0);
    }

    @Test
    void testCompareToConsistentWithEquals() {
        ObjectPath path1 = new ObjectPath("db", "table");
        ObjectPath path2 = new ObjectPath("db", "table");

        // compareTo == 0 should be consistent with equals
        assertThat(path1.compareTo(path2) == 0).isEqualTo(path1.equals(path2));
    }

    @Test
    void testSortingWithCollections() {
        ObjectPath path1 = new ObjectPath("cDb", "zTable");
        ObjectPath path2 = new ObjectPath("aDb", "mTable");
        ObjectPath path3 = new ObjectPath("bDb", "aTable");
        ObjectPath path4 = new ObjectPath("aDb", "aTable");

        List<ObjectPath> paths = Arrays.asList(path1, path2, path3, path4);
        paths.sort(null);

        // After sorting: aDb.aTable, aDb.mTable, bDb.aTable, cDb.zTable
        assertThat(paths).containsExactly(path4, path2, path3, path1);
    }

    @Test
    void testCompareToTransitivity() {
        ObjectPath path1 = new ObjectPath("aDb", "aTable");
        ObjectPath path2 = new ObjectPath("bDb", "aTable");
        ObjectPath path3 = new ObjectPath("cDb", "aTable");

        // Transitivity: path1 < path2 and path2 < path3, then path1 < path3
        assertThat(path1.compareTo(path2)).isLessThan(0);
        assertThat(path2.compareTo(path3)).isLessThan(0);
        assertThat(path1.compareTo(path3)).isLessThan(0);
    }

    @Test
    void testCompareToDatabaseNameTakesPrecedence() {
        // Even if objectName order is reversed, databaseName comparison takes precedence
        ObjectPath path1 = new ObjectPath("aDb", "zTable");
        ObjectPath path2 = new ObjectPath("bDb", "aTable");

        assertThat(path1.compareTo(path2)).isLessThan(0);
    }
}
