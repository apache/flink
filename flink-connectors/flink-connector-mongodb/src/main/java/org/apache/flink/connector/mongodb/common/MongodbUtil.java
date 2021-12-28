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
