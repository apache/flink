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

package org.apache.flink.connector.elasticsearch.sink;

import javax.annotation.Nullable;

import java.io.Serializable;

class NetworkClientConfig implements Serializable {

    @Nullable private final String username;
    @Nullable private final String password;
    @Nullable private final String connectionPathPrefix;
    @Nullable private final Integer connectionRequestTimeout;
    @Nullable private final Integer connectionTimeout;
    @Nullable private final Integer socketTimeout;

    NetworkClientConfig(
            @Nullable String username,
            @Nullable String password,
            @Nullable String connectionPathPrefix,
            @Nullable Integer connectionRequestTimeout,
            @Nullable Integer connectionTimeout,
            @Nullable Integer socketTimeout) {
        this.username = username;
        this.password = password;
        this.connectionPathPrefix = connectionPathPrefix;
        this.connectionRequestTimeout = connectionRequestTimeout;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    @Nullable
    public Integer getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    @Nullable
    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    @Nullable
    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    @Nullable
    public String getConnectionPathPrefix() {
        return connectionPathPrefix;
    }
}
