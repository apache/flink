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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides bucket-specific S3 configurations from Flink config. Format:
 * s3.bucket.&lt;bucket-name&gt;.&lt;property&gt; (e.g. s3.bucket.my-bucket.path-style-access: true)
 */
@Internal
public class BucketConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(BucketConfigProvider.class);

    private static final String BUCKET_CONFIG_PREFIX = "s3.bucket.";

    private static final String[] KNOWN_PROPERTIES =
            new String[] {
                "assume-role.external-id",
                "assume-role.arn",
                "sse.kms-key-id",
                "path-style-access",
                "sse.type",
                "access-key",
                "secret-key",
                "endpoint",
                "region"
            };

    private final Map<String, S3BucketConfig> bucketConfigs = new HashMap<>();

    public BucketConfigProvider(Configuration flinkConfig) {
        parseBucketConfigs(flinkConfig);
    }

    private void parseBucketConfigs(Configuration flinkConfig) {
        Map<String, Map<String, String>> bucketConfigMap = new HashMap<>();

        for (String key : flinkConfig.keySet()) {
            if (key.startsWith(BUCKET_CONFIG_PREFIX)) {
                String suffix = key.substring(BUCKET_CONFIG_PREFIX.length());
                String value = flinkConfig.getString(key, null);
                if (value == null) {
                    continue;
                }
                for (String prop : KNOWN_PROPERTIES) {
                    if (suffix.endsWith("." + prop)) {
                        String bucketName =
                                suffix.substring(0, suffix.length() - prop.length() - 1);
                        if (!bucketName.isEmpty()) {
                            bucketConfigMap
                                    .computeIfAbsent(bucketName, k -> new HashMap<>())
                                    .put(prop, value);
                        }
                        break;
                    }
                }
            }
        }

        for (Map.Entry<String, Map<String, String>> entry : bucketConfigMap.entrySet()) {
            String bucketName = entry.getKey();
            Map<String, String> configMap = entry.getValue();

            S3BucketConfig.Builder builder = S3BucketConfig.builder(bucketName);

            if (configMap.containsKey("path-style-access")) {
                builder.pathStyleAccess(Boolean.parseBoolean(configMap.get("path-style-access")));
            }

            if (configMap.containsKey("endpoint")) {
                builder.endpoint(configMap.get("endpoint"));
            }

            if (configMap.containsKey("region")) {
                builder.region(configMap.get("region"));
            }

            if (configMap.containsKey("access-key")) {
                builder.accessKey(configMap.get("access-key"));
            }

            if (configMap.containsKey("secret-key")) {
                builder.secretKey(configMap.get("secret-key"));
            }

            if (configMap.containsKey("sse.type")) {
                builder.sseType(configMap.get("sse.type"));
            }

            if (configMap.containsKey("sse.kms-key-id")) {
                builder.sseKmsKeyId(configMap.get("sse.kms-key-id"));
            }

            if (configMap.containsKey("assume-role.arn")) {
                builder.assumeRoleArn(configMap.get("assume-role.arn"));
            }

            if (configMap.containsKey("assume-role.external-id")) {
                builder.assumeRoleExternalId(configMap.get("assume-role.external-id"));
            }

            S3BucketConfig bucketConfig = builder.build();
            bucketConfigs.put(bucketName, bucketConfig);

            LOG.info("Registered bucket-specific configuration for bucket: {}", bucketName);
        }
    }

    /** Returns bucket config if defined, null otherwise. */
    @Nullable
    public S3BucketConfig getBucketConfig(String bucketName) {
        return bucketConfigs.get(bucketName);
    }

    public boolean hasBucketConfig(String bucketName) {
        return bucketConfigs.containsKey(bucketName);
    }
}
