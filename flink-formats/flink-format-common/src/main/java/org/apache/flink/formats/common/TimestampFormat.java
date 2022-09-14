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

package org.apache.flink.formats.common;

import org.apache.flink.annotation.Internal;

/** Timestamp format Enums. */
@Internal
public enum TimestampFormat {
    /**
     * Options to specify TIMESTAMP/TIMESTAMP_WITH_LOCAL_ZONE format. It will parse TIMESTAMP in
     * "yyyy-MM-dd HH:mm:ss.s{precision}" format, TIMESTAMP_WITH_LOCAL_TIMEZONE in "yyyy-MM-dd
     * HH:mm:ss.s{precision}'Z'" and output in the same format.
     */
    SQL,

    /**
     * Options to specify TIMESTAMP/TIMESTAMP_WITH_LOCAL_ZONE format. It will parse TIMESTAMP in
     * "yyyy-MM-ddTHH:mm:ss.s{precision}" format, TIMESTAMP_WITH_LOCAL_TIMEZONE in
     * "yyyy-MM-ddTHH:mm:ss.s{precision}'Z'" and output in the same format.
     */
    ISO_8601
}
