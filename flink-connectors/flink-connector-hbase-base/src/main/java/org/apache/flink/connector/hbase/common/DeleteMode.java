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

package org.apache.flink.connector.hbase.common;

import org.apache.flink.annotation.Internal;

/** Delete mode enums for the HBase sink. */
@Internal
public enum DeleteMode {
    /**
     * Options to specify delete mode. When {@link org.apache.flink.table.connector.ChangelogMode}
     * is UPSERT or ALL, it will only delete the latest version of a specified key in HBase and
     * retain the previous versions.
     */
    LATEST_VERSION,

    /**
     * Options to specify delete mode. When {@link org.apache.flink.table.connector.ChangelogMode}
     * is UPSERT or ALL, it will delete all versions of a specified key in HBase.
     */
    ALL_VERSIONS
}
