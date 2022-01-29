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
import org.apache.flink.connector.mongodb.config.MongoConfigConstants;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import java.util.Properties;

import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.CONNECTION_STRING;
import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM;
import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.MONGO_CREDENTIAL_PASSWORD;
import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.MONGO_CREDENTIAL_SOURCE;
import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.MONGO_CREDENTIAL_USERNAME;
import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.MONGO_SERVER_API_DEPRECATION_ERRORS;
import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.MONGO_SERVER_API_STRICT;
import static org.apache.flink.connector.mongodb.config.MongoConfigConstants.MONGO_SERVER_API_VERSION;

/** Some utilities specific to MongoDb Client. */
@Internal
public class MongoGeneralUtil {

    public static MongoClient createMongoClient(final Properties configProperties) {
        return MongoClients.create(getMongoClientSettings(configProperties));
    }

    public static MongoClientSettings getMongoClientSettings(final Properties configProperties) {
        final MongoClientSettings.Builder builder =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(
                                        configProperties.getProperty(CONNECTION_STRING)));
        if (configProperties.containsKey(MONGO_CREDENTIAL_MECHANISM)) {
            builder.credential(getMongoCredential(configProperties));
        }
        if (configProperties.containsKey(MONGO_SERVER_API_VERSION)) {
            builder.serverApi(getServerApi(configProperties));
        }
        return builder.build();
    }

    public static MongoCredential getMongoCredential(final Properties configProperties) {
        MongoConfigConstants.MongoAuthenticationMechanism credentialMechanism =
                MongoConfigConstants.MongoAuthenticationMechanism.valueOf(
                        configProperties.getProperty(MONGO_CREDENTIAL_MECHANISM));
        String userName = configProperties.getProperty(MONGO_CREDENTIAL_USERNAME);
        String source = configProperties.getProperty(MONGO_CREDENTIAL_SOURCE);
        String password = configProperties.getProperty(MONGO_CREDENTIAL_PASSWORD);
        switch (credentialMechanism) {
            case MONGODB_X509:
                if (userName != null) {
                    return MongoCredential.createMongoX509Credential(userName);
                } else {
                    return MongoCredential.createMongoX509Credential();
                }
            case GSSAPI:
                return MongoCredential.createGSSAPICredential(userName);
            case SCRAM_SHA_1:
                return MongoCredential.createScramSha1Credential(
                        userName, source, password.toCharArray());
            case SCRAM_SHA_256:
                return MongoCredential.createScramSha256Credential(
                        userName, source, password.toCharArray());
            case PLAIN:
                return MongoCredential.createPlainCredential(
                        userName, source, password.toCharArray());
            case MONGODB_AWS:
                return MongoCredential.createAwsCredential(userName, password.toCharArray());
        }
        throw new IllegalArgumentException("Unknown Mongo Credential Mechanism");
    }

    public static ServerApi getServerApi(final Properties configProperties) {
        ServerApi.Builder builder = ServerApi.builder();
        builder.version(
                ServerApiVersion.valueOf(configProperties.getProperty(MONGO_SERVER_API_VERSION)));
        if (configProperties.containsKey(MONGO_SERVER_API_STRICT)) {
            builder.strict(
                    Boolean.parseBoolean(configProperties.getProperty(MONGO_SERVER_API_STRICT)));
        }
        if (configProperties.containsKey(MONGO_SERVER_API_DEPRECATION_ERRORS)) {
            builder.deprecationErrors(
                    Boolean.parseBoolean(
                            configProperties.getProperty(MONGO_SERVER_API_DEPRECATION_ERRORS)));
        }
        return builder.build();
    }
}
