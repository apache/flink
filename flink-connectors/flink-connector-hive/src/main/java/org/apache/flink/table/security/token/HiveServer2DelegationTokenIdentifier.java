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

package org.apache.flink.table.security.token;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/** Delegation token identifier for HiveServer2. */
@Internal
public class HiveServer2DelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
    public static final Text HIVE_DELEGATION_KIND = new Text("HIVE_DELEGATION_TOKEN");

    public HiveServer2DelegationTokenIdentifier() {}

    public HiveServer2DelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
        super(owner, renewer, realUser);
    }

    public Text getKind() {
        return HIVE_DELEGATION_KIND;
    }
}
