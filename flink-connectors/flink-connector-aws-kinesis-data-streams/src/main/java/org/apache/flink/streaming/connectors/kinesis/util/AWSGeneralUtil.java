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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;

import java.util.Properties;

/** Some utilities specific to Amazon Web Service. */
@Internal
public class AWSGeneralUtil {
    /** Used for formatting Flink-specific user agent string when creating Kinesis client. */
    private static final String USER_AGENT_FORMAT = "Apache Flink %s (%s) Kinesis Connector";

    /**
     * Creates a user agent prefix for Flink. This can be used by HTTP Clients.
     *
     * @return a user agent prefix for Flink
     */
    public static String formatFlinkUserAgentPrefix() {
        return String.format(
                USER_AGENT_FORMAT,
                EnvironmentInformation.getVersion(),
                EnvironmentInformation.getRevisionInformation().commitId);
    }

    /**
     * Determines and returns the credential provider type from the given properties.
     *
     * @return the credential provider type
     */
    static CredentialProvider getCredentialProviderType(
            final Properties configProps, final String configPrefix) {
        if (!configProps.containsKey(configPrefix)) {
            if (configProps.containsKey(AWSConfigConstants.accessKeyId(configPrefix))
                    && configProps.containsKey(AWSConfigConstants.secretKey(configPrefix))) {
                // if the credential provider type is not specified, but the Access Key ID and
                // Secret Key are given, it will default to BASIC
                return CredentialProvider.BASIC;
            } else {
                // if the credential provider type is not specified, it will default to AUTO
                return CredentialProvider.AUTO;
            }
        } else {
            return CredentialProvider.valueOf(configProps.getProperty(configPrefix));
        }
    }
}
