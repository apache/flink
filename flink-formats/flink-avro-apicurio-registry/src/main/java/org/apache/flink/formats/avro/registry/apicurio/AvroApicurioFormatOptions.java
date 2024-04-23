/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import io.apicurio.registry.serde.SerdeConfig;

import java.time.Duration;
import java.util.Map;

/** Options for Schema Registry Avro format. */
@PublicEvolving
public class AvroApicurioFormatOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The URL of the Apicurio Registry to fetch/register schemas.");

    public static final ConfigOption<Boolean> USE_HEADERS =
            ConfigOptions.key("use-headers")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Used by serializers and deserializers. Configures the  identifier for artifacts. \n"
                                    + "true means use global ID, false means use content ID.\n"
                                    + "Instructs the serializer to write the specified ID to Kafka,\n"
                                    + "and instructs the deserializer to use this ID to find the schema.");

    public static final ConfigOption<Boolean> USE_GLOBALID =
            ConfigOptions.key("use-globalid")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Configures to read/write the artifact identifier to "
                                    + "Kafka message headers instead of in the message payload.");

    public static final ConfigOption<String> GROUP_ID =
            ConfigOptions.key("groupId")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("GroupId Used by deserializers.");

    public static final ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The schema registered or to be registered in the Apicurio Registry. "
                                    + "If no schema is provided Flink converts the table schema to avro schema. "
                                    + "The schema provided must match the table schema ('avro-apicurio').");
    public static final ConfigOption<String> REGISTERED_ARTIFACT_ID =
            ConfigOptions.key("artifactId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "ArtifactId is used by serializers when registering a new Schemas to ensure that\n"
                                    + "each serialization is associated with the same schema in the registry.\n"
                                    + "It is often useful to include the topic name in the artifact id so\n"
                                    + "the schema can be identified with its associated topic; the default is the topic name.");
    public static final ConfigOption<String> REGISTERED_ARTIFACT_NAME =
            ConfigOptions.key("artifactName")
                    .stringType()
                    .defaultValue("generated-schema")
                    .withDescription(
                            "The registered artifact name is used by serializers as the name of the schema being registered; \n"
                                    + "the default is the topic name.");
    public static final ConfigOption<String> REGISTERED_ARTIFACT_DESCRIPTION =
            ConfigOptions.key("artifactDescription")
                    .stringType()
                    .defaultValue("Schema registered by Apache Flink.")
                    .withDescription(
                            "The registered schema description is used by serializers as the description of the schema being registered");
    public static final ConfigOption<String> REGISTERED_ARTIFACT_VERSION =
            ConfigOptions.key("artifactVersion")
                    .stringType()
                    .defaultValue("1")
                    .withDescription(
                            "The registered artifact version is used by serializers as the version of the schema being registered");

    // --------------------------------------------------------------------------------------------
    // Commonly used options maintained by Flink for convenience
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SSL_KEYSTORE_LOCATION =
            ConfigOptions.key(SerdeConfig.REQUEST_KEYSTORE_LOCATION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Location / File of SSL keystore");

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
            ConfigOptions.key(SerdeConfig.REQUEST_KEYSTORE_PASSWORD)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for SSL keystore");
    public static final ConfigOption<String> SSL_KEYSTORE_TYPE =
            ConfigOptions.key(SerdeConfig.REQUEST_KEYSTORE_TYPE)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Type for SSL truststore");

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION =
            ConfigOptions.key(SerdeConfig.REQUEST_TRUSTSTORE_LOCATION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Location / File of SSL truststore");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
            ConfigOptions.key(SerdeConfig.REQUEST_TRUSTSTORE_PASSWORD)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for SSL truststore");

    public static final ConfigOption<String> SSL_TRUSTSTORE_TYPE =
            ConfigOptions.key(SerdeConfig.REQUEST_TRUSTSTORE_TYPE)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Type for SSL truststore");

    public static final ConfigOption<String> BASIC_AUTH_CREDENTIALS_USERID =
            ConfigOptions.key(SerdeConfig.AUTH_USERNAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Basic auth userid for Apicurio Registry");

    public static final ConfigOption<String> BASIC_AUTH_CREDENTIALS_PASSWORD =
            ConfigOptions.key(SerdeConfig.AUTH_PASSWORD)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Basic auth password for Apicurio Registry");

    public static final ConfigOption<String> OIDC_AUTH_URL =
            ConfigOptions.key("OIDC_AUTH_URL")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OIDC endpoint for Authorization.");

    public static final ConfigOption<String> OIDC_AUTH_CLIENT_ID =
            ConfigOptions.key("OIDC_AUTH_CLIENT_ID")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OIDC Client ID for Apicurio Registry.");

    public static final ConfigOption<String> OIDC_AUTH_CLIENT_SECRET =
            ConfigOptions.key("OIDC_AUTH_CLIENT_SECRET")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OIDC Client SECRET for Apicurio Registry.");

    public static final ConfigOption<String> OIDC_AUTH_SCOPE =
            ConfigOptions.key("OIDC_AUTH_SCOPE")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OIDC scope for Apicurio Registry.");

    public static final ConfigOption<Duration> OIDC_AUTH_TOKEN_EXPIRATION_REDUCTION =
            ConfigOptions.key("tokenExpirationReduction")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("OIDC token expiry reduction for Apicurio Registry.");

    // TODO the other SerdeConfig. options

    // --------------------------------------------------------------------------------------------
    // Fallback properties
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Map<String, String>> PROPERTIES =
            ConfigOptions.key("properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Properties map that is forwarded to the underlying Schema Registry. "
                                    + "This is useful for options that are not officially exposed "
                                    + "via Flink config options. However, note that Flink options "
                                    + "have higher precedence.");

    private AvroApicurioFormatOptions() {}
}
