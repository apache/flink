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

package org.apache.flink.connector.mongodb.common;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Specify MongoClient Settingsicons. */
public class MongodbUtil {

    public static MongoClientSettings createMongoClientSettings(final Properties configProperties) {
        Builder builder = MongoClientSettings.builder();

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.READ_CONCERN))
                .ifPresent(r -> builder.readConcern(new ReadConcern(ReadConcernLevel.valueOf(r))));

        Optional.ofNullable(
                        configProperties.getProperty(
                                MongodbConfigConstants.APPLY_CONNECTION_STRING))
                .ifPresent(r -> builder.applyConnectionString(new ConnectionString(r)));

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.RETRYABLE_WRITES))
                .ifPresent(r -> builder.retryWrites(Boolean.parseBoolean(r)));

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.RETRYABLE_READS))
                .ifPresent(r -> builder.retryReads(Boolean.parseBoolean(r)));

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.WRITE_CONCERN))
                .ifPresent(r -> builder.writeConcern(new WriteConcern(r)));

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.READ_PREFERENCE))
                .ifPresent(r -> builder.readPreference(ReadPreference.valueOf(r)));

        Optional.ofNullable(
                        configProperties.getProperty(
                                MongodbConfigConstants.SERVER_SELECTION_TIMEOUT))
                .ifPresent(
                        r ->
                                builder.applyToClusterSettings(
                                        b ->
                                                b.serverSelectionTimeout(
                                                        Long.parseLong(r), TimeUnit.MILLISECONDS)));

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.MAX_SIZE))
                .ifPresent(
                        r ->
                                builder.applyToConnectionPoolSettings(
                                        b -> b.maxSize(Integer.parseInt(r))));

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.MIN_SIZE))
                .ifPresent(
                        r ->
                                builder.applyToConnectionPoolSettings(
                                        b -> b.minSize(Integer.parseInt(r))));

        Optional.ofNullable(configProperties.getProperty(MongodbConfigConstants.MAX_WAIT_TIME))
                .ifPresent(
                        r ->
                                builder.applyToConnectionPoolSettings(
                                        b ->
                                                b.maxWaitTime(
                                                        Long.parseLong(r), TimeUnit.MILLISECONDS)));
        return builder.build();
    }
}
