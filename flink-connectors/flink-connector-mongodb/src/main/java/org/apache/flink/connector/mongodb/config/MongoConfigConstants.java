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

package org.apache.flink.connector.mongodb.config;

import org.apache.flink.annotation.PublicEvolving;

/** Configuration keys for MongoDb client usage. */
@PublicEvolving
public class MongoConfigConstants {

    /**
     * Possible configuration values for the type of credential mechanism to use when accessing
     * MongoDb. Internally, a corresponding implementation of {@link
     * com.mongodb.AuthenticationMechanism} will be used.
     */
    public enum MongoAuthenticationMechanism {
        GSSAPI,
        PLAIN,
        MONGODB_X509,
        SCRAM_SHA_1,
        SCRAM_SHA_256,
        MONGODB_AWS
    }

    public static final String CONNECTION_STRING = "mongodb.connection_string";
    public static final String MONGO_CREDENTIAL_MECHANISM = "mongodb.credential_mechanism";
    public static final String MONGO_CREDENTIAL_SOURCE = "mongodb.credential.source";
    public static final String MONGO_CREDENTIAL_USERNAME = "mongodb.credential.user_name";
    public static final String MONGO_CREDENTIAL_PASSWORD = "mongodb.credential.password";
    public static final String MONGO_SERVER_API_VERSION = "mongodb.version_api.version";
    public static final String MONGO_SERVER_API_STRICT = "mongodb.version_api.strict";
    public static final String MONGO_SERVER_API_DEPRECATION_ERRORS =
            "mongodb.version_api.deprecation_errors";
}
