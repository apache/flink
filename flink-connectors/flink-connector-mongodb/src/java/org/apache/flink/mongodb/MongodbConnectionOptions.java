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

package org.apache.flink.mongodb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** Mongodb connection options. */
@PublicEvolving
public class MongodbConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String url;
    protected final int connectionCheckTimeoutSeconds;
    @Nullable protected final String username;
    @Nullable protected final String password;

    protected MongodbConnectionOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            int connectionCheckTimeoutSeconds) {
        Preconditions.checkArgument(connectionCheckTimeoutSeconds > 0);
        this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
        this.username = username;
        this.password = password;
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
    }

    public String getDbURL() {
        return url;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    /** Builder for {@link MongodbConnectionOptions}. */
    public static class JdbcConnectionOptionsBuilder {
        private String url;
        private String username;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;

        public JdbcConnectionOptionsBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public JdbcConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public JdbcConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Set the maximum timeout between retries, default is 60 seconds.
         *
         * @param connectionCheckTimeoutSeconds the timeout seconds, shouldn't smaller than 1
         *     second.
         */
        public JdbcConnectionOptionsBuilder withConnectionCheckTimeoutSeconds(
                int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public MongodbConnectionOptions build() {
            return new MongodbConnectionOptions(
                    url, username, password, connectionCheckTimeoutSeconds);
        }
    }
}
