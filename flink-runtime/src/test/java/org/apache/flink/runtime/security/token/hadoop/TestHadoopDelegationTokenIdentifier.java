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

package org.apache.flink.runtime.security.token.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Example test implementation of {@link AbstractDelegationTokenIdentifier} which is used in
 * integration tests.
 */
public class TestHadoopDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {

    private static final Text tokenKind = new Text("TEST_TOKEN_KIND");

    private long issueDate;

    // This is needed for service loader
    public TestHadoopDelegationTokenIdentifier() {}

    public TestHadoopDelegationTokenIdentifier(long issueDate) {
        this.issueDate = issueDate;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(issueDate);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        issueDate = in.readLong();
    }

    @Override
    public Text getKind() {
        return tokenKind;
    }

    @Override
    public long getIssueDate() {
        return issueDate;
    }
}
