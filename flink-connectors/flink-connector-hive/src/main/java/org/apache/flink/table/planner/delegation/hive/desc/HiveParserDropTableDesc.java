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

package org.apache.flink.table.planner.delegation.hive.desc;

import java.io.Serializable;

/** Desc for DROP TABLE/VIEW operation. */
public class HiveParserDropTableDesc implements Serializable {
    private static final long serialVersionUID = 7493000830476614290L;

    private final String compoundName;
    private final boolean expectView;
    private final boolean ifExists;
    private final boolean purge;

    public HiveParserDropTableDesc(
            String compoundName, boolean expectView, boolean ifExists, boolean purge) {
        this.compoundName = compoundName;
        this.expectView = expectView;
        this.ifExists = ifExists;
        this.purge = purge;
    }

    public String getCompoundName() {
        return compoundName;
    }

    public boolean ifExists() {
        return ifExists;
    }

    public boolean isPurge() {
        return purge;
    }

    public boolean isExpectView() {
        return expectView;
    }
}
