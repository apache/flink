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

package org.apache.flink.connector.file.table;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowPartitionComputer}. */
class RowPartitionComputerTest {

    @Test
    void testProjectColumnsToWrite() {
        Row projected1 =
                new RowPartitionComputer(
                                "",
                                new String[] {"f1", "p1", "p2", "f2"},
                                new String[] {"p1", "p2"})
                        .projectColumnsToWrite(Row.of(1, 2, 3, 4));
        assertThat(projected1).isEqualTo(Row.of(1, 4));

        Row projected2 =
                new RowPartitionComputer(
                                "",
                                new String[] {"f1", "f2", "p1", "p2"},
                                new String[] {"p1", "p2"})
                        .projectColumnsToWrite(Row.of(1, 2, 3, 4));
        assertThat(projected2).isEqualTo(Row.of(1, 2));

        Row projected3 =
                new RowPartitionComputer(
                                "",
                                new String[] {"f1", "p1", "f2", "p2"},
                                new String[] {"p1", "p2"})
                        .projectColumnsToWrite(Row.of(1, 2, 3, 4));
        assertThat(projected3).isEqualTo(Row.of(1, 3));
    }

    @Test
    void testComputePartition() throws Exception {
        RowPartitionComputer computer =
                new RowPartitionComputer(
                        "myDefaultname",
                        new String[] {"f1", "p1", "p2", "f2"},
                        new String[] {"p1", "p2"});
        assertThat(generatePartitionPath(computer.generatePartValues(Row.of(1, 2, 3, 4))))
                .isEqualTo("p1=2/p2=3/");
        assertThat(generatePartitionPath(computer.generatePartValues(Row.of(1, null, 3, 4))))
                .isEqualTo("p1=myDefaultname/p2=3/");
    }
}
