package org.apache.flink.connector.file.table;

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

import java.util.HashSet;
import java.util.Set;

/** A custom PartitionCommitPolicy for test. */
public class TestCustomCommitPolicy implements PartitionCommitPolicy {

    private String param1;
    private String param2;

    public TestCustomCommitPolicy() {}

    public TestCustomCommitPolicy(String param1, String param2) {
        this.param1 = param1;
        this.param2 = param2;
    }

    public String getParam1() {
        return param1;
    }

    public String getParam2() {
        return param2;
    }

    private static Set<String> committedPartitionPaths = new HashSet<>();

    @Override
    public void commit(PartitionCommitPolicy.Context context) throws Exception {
        TestCustomCommitPolicy.committedPartitionPaths.add(context.partitionPath().getPath());
        TestCustomCommitPolicy.committedPartitionPaths.add(param1 + param2);
    }

    static Set<String> getCommittedPartitionPathsAndReset() {
        Set<String> paths = TestCustomCommitPolicy.committedPartitionPaths;
        TestCustomCommitPolicy.committedPartitionPaths = new HashSet<>();
        return paths;
    }
}
