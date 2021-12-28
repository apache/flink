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

package org.apache.flink.connector.aws.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.internal.NettyConfiguration;

import java.lang.reflect.Field;
import java.util.Properties;

/** Utilities for tests in the package. */
public class TestUtil {
    public static Properties properties(final String key, final String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return properties;
    }

    public static Properties getStandardProperties() {
        Properties config = properties(AWSConfigConstants.AWS_REGION, "us-east-1");
        config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
        config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        return config;
    }

    public static NettyConfiguration getNettyConfiguration(final SdkAsyncHttpClient httpClient)
            throws Exception {
        return getField("configuration", httpClient);
    }

    public static <T> T getField(String fieldName, Object obj) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }
}
