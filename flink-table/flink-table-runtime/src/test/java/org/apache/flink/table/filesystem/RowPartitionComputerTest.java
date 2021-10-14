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

package org.apache.flink.table.filesystem;

import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/** Test for {@link RowPartitionComputer}. */
public class RowPartitionComputerTest {

    @Test
    public void testProjectColumnsToWrite() throws Exception {
        Row projected1 =
                new RowPartitionComputer(
                                "",
                                new String[] {"f1", "p1", "p2", "f2"},
                                new String[] {"p1", "p2"})
                        .projectColumnsToWrite(Row.of(1, 2, 3, 4));
        Assert.assertEquals(Row.of(1, 4), projected1);

        Row projected2 =
                new RowPartitionComputer(
                                "",
                                new String[] {"f1", "f2", "p1", "p2"},
                                new String[] {"p1", "p2"})
                        .projectColumnsToWrite(Row.of(1, 2, 3, 4));
        Assert.assertEquals(Row.of(1, 2), projected2);

        Row projected3 =
                new RowPartitionComputer(
                                "",
                                new String[] {"f1", "p1", "f2", "p2"},
                                new String[] {"p1", "p2"})
                        .projectColumnsToWrite(Row.of(1, 2, 3, 4));
        Assert.assertEquals(Row.of(1, 3), projected3);
    }

    @Test
    public void testComputePartition() throws Exception {
        RowPartitionComputer computer =
                new RowPartitionComputer(
                        "myDefaultname",
                        new String[] {"f1", "p1", "p2", "f2"},
                        new String[] {"p1", "p2"});
        Assert.assertEquals(
                "p1=2/p2=3/",
                generatePartitionPath(computer.generatePartValues(Row.of(1, 2, 3, 4))));
        Assert.assertEquals(
                "p1=myDefaultname/p2=3/",
                generatePartitionPath(computer.generatePartValues(Row.of(1, null, 3, 4))));
    }
}
