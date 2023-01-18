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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;

import javax.annotation.Nullable;

/** Utils for the {@link SqlGatewayRestEndpoint}. */
public class SqlGatewayRestEndpointUtils {

    /** Parse token from the result uri. */
    public static @Nullable Long parseToken(@Nullable String nextResultUri) {
        if (nextResultUri == null || nextResultUri.length() == 0) {
            return null;
        }
        String[] split = nextResultUri.split("/");
        // remove query string
        String s = split[split.length - 1];
        s = s.replaceAll("\\?.*", "");
        return Long.valueOf(s);
    }
}
