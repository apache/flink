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

package org.apache.flink.connector.mongodb.util;

import org.apache.flink.annotation.Internal;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Utils for MongoDB. */
@Internal
public class MongoUtils {

    public static BulkWriteOptions createBulkWriteOptions(Properties configProperties) {
        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        Optional.ofNullable(
                        configProperties.getProperty(
                                MongoDbConfigConstants.BULK_WRITE_OPTIONS_ORDERED))
                .ifPresent(r -> bulkWriteOptions.ordered(Boolean.parseBoolean(r)));
        Optional.ofNullable(
                        configProperties.getProperty(
                                MongoDbConfigConstants
                                        .BULK_WRITE_OPTIONS_BYPASS_DOCUMENT_VALIDATION))
                .ifPresent(r -> bulkWriteOptions.bypassDocumentValidation(Boolean.parseBoolean(r)));
        return bulkWriteOptions;
    }

    public static MongoClientSettings createMongoClientSettings(final Properties configProperties) {
        MongoClientSettings.Builder builder = MongoClientSettings.builder();

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.READ_CONCERN))
                .ifPresent(r -> builder.readConcern(new ReadConcern(ReadConcernLevel.valueOf(r))));

        Optional.ofNullable(
                        configProperties.getProperty(
                                MongoDbConfigConstants.APPLY_CONNECTION_STRING))
                .ifPresent(r -> builder.applyConnectionString(new ConnectionString(r)));

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.RETRYABLE_WRITES))
                .ifPresent(r -> builder.retryWrites(Boolean.parseBoolean(r)));

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.RETRYABLE_READS))
                .ifPresent(r -> builder.retryReads(Boolean.parseBoolean(r)));

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.WRITE_CONCERN))
                .ifPresent(r -> builder.writeConcern(new WriteConcern(r)));

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.READ_PREFERENCE))
                .ifPresent(r -> builder.readPreference(ReadPreference.valueOf(r)));

        Optional.ofNullable(
                        configProperties.getProperty(
                                MongoDbConfigConstants.SERVER_SELECTION_TIMEOUT))
                .ifPresent(
                        r ->
                                builder.applyToClusterSettings(
                                        b ->
                                                b.serverSelectionTimeout(
                                                        Long.parseLong(r), TimeUnit.MILLISECONDS)));

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.MAX_SIZE))
                .ifPresent(
                        r ->
                                builder.applyToConnectionPoolSettings(
                                        b -> b.maxSize(Integer.parseInt(r))));

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.MIN_SIZE))
                .ifPresent(
                        r ->
                                builder.applyToConnectionPoolSettings(
                                        b -> b.minSize(Integer.parseInt(r))));

        Optional.ofNullable(configProperties.getProperty(MongoDbConfigConstants.MAX_WAIT_TIME))
                .ifPresent(
                        r ->
                                builder.applyToConnectionPoolSettings(
                                        b ->
                                                b.maxWaitTime(
                                                        Long.parseLong(r), TimeUnit.MILLISECONDS)));
        return builder.build();
    }

    public static MongoClient createMongoClient(final Properties mongoProperties) {
        MongoClientSettings mongoClientSettings = createMongoClientSettings(mongoProperties);
        return MongoClients.create(mongoClientSettings);
    }
}
