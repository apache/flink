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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.filesystem.PartitionCommitPolicy;

import java.util.HashSet;
import java.util.Set;

/** A custom PartitionCommitPolicy for test. */
public class TestCustomCommitPolicy implements PartitionCommitPolicy {

    private static Set<String> committedPartitionPaths = new HashSet<>();

    @Override
    public void commit(PartitionCommitPolicy.Context context) throws Exception {
        TestCustomCommitPolicy.committedPartitionPaths.add(context.partitionPath().getPath());
    }

    static Set<String> getCommittedPartitionPathsAndReset() {
        Set<String> paths = TestCustomCommitPolicy.committedPartitionPaths;
        TestCustomCommitPolicy.committedPartitionPaths = new HashSet<>();
        return paths;
    }
}
