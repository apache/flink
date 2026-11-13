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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory;
import org.apache.flink.fs.s3.common.token.AbstractS3DelegationTokenReceiver;
import org.apache.flink.fs.s3.common.token.HadoopDynamicTemporaryAWSCredentialsProvider;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/** Simple factory for the S3 file system. */
public class S3FileSystemFactory extends AbstractS3FileSystemFactory {

    private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemFactory.class);

    private static final String[] FLINK_CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    /**
     * AWS SDK v1 credentials providers that are commonly configured and that hadoop-aws doesn't map
     * to AWS SDK v2 itself. This plugin no longer bundles AWS SDK v1.
     */
    @VisibleForTesting
    static final Map<String, String> SDK_V1_PROVIDER_MAPPING = createSdkV1ProviderMapping();

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"},
        // Flink v2 uses standard YAML which cannot represent a key that is both a scalar value and
        // a parent of other keys (e.g. fs.s3a.endpoint and fs.s3a.endpoint.region conflict).
        // These aliases allow users to set the scalar key via a ".value" suffix to avoid the
        // collision, e.g. fs.s3a.endpoint.value maps to fs.s3a.endpoint in Hadoop config.
        {"fs.s3a.endpoint.value", "fs.s3a.endpoint"},
        {"fs.s3a.assumed.role.sts.endpoint.value", "fs.s3a.assumed.role.sts.endpoint"},
        {"fs.s3a.fast.upload.value", "fs.s3a.fast.upload"},
        {"fs.s3a.multipart.purge.value", "fs.s3a.multipart.purge"},
        {"fs.s3a.s3guard.ddb.table.value", "fs.s3a.s3guard.ddb.table"},
    };

    public S3FileSystemFactory() {
        super("Hadoop s3a file system", createHadoopConfigLoader());
    }

    @Override
    public String getScheme() {
        return "s3";
    }

    @VisibleForTesting
    static HadoopConfigLoader createHadoopConfigLoader() {
        return new HadoopConfigLoader(
                FLINK_CONFIG_PREFIXES,
                MIRRORED_CONFIG_KEYS,
                "fs.s3a.",
                Collections.emptySet(),
                Collections.emptySet(),
                "");
    }

    @Override
    protected org.apache.hadoop.fs.FileSystem createHadoopFileSystem() {
        return new S3AFileSystem();
    }

    @Override
    protected URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
        final String scheme = fsUri.getScheme();
        final String authority = fsUri.getAuthority();

        if (scheme == null && authority == null) {
            fsUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
        } else if (scheme != null && authority == null) {
            URI defaultUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
            if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                fsUri = defaultUri;
            }
        }

        LOG.debug("Using scheme {} for s3a file system backing the S3 File System", fsUri);

        return fsUri;
    }

    @Nullable
    @Override
    protected S3AccessHelper getS3AccessHelper(FileSystem fs) {
        final S3AFileSystem s3Afs = (S3AFileSystem) fs;
        return new HadoopS3AccessHelper(s3Afs, s3Afs.getConf());
    }

    @Override
    protected void applyHadoopConfigDefaults(org.apache.hadoop.conf.Configuration hadoopConfig) {
        // hadoop-aws 3.4.3 defaults to the Analytics Accelerator stream, which changes how every
        // s3a:// read hits S3. Keep the classic stream unless the user opts in. Hadoop gives the
        // legacy prefetch toggle precedence when no stream type is set, so leave that case alone.
        if (hadoopConfig.get(Constants.INPUT_STREAM_TYPE) == null
                && !hadoopConfig.getBoolean(
                        Constants.PREFETCH_ENABLED_KEY, Constants.PREFETCH_ENABLED_DEFAULT)) {
            hadoopConfig.set(Constants.INPUT_STREAM_TYPE, Constants.INPUT_STREAM_TYPE_CLASSIC);
        }
        addSdkV1ProviderMapping(hadoopConfig);
    }

    /** Adds {@link #SDK_V1_PROVIDER_MAPPING} for the providers the user hasn't mapped. */
    @VisibleForTesting
    static void addSdkV1ProviderMapping(org.apache.hadoop.conf.Configuration hadoopConfig) {
        final Set<String> mappedProviders =
                S3AUtils.getTrimmedStringCollectionSplitByEquals(
                                hadoopConfig, Constants.AWS_CREDENTIALS_PROVIDER_MAPPING)
                        .keySet();
        final StringJoiner mapping = new StringJoiner(",");
        final String configured =
                hadoopConfig.getTrimmed(Constants.AWS_CREDENTIALS_PROVIDER_MAPPING, "");
        if (!configured.isEmpty()) {
            mapping.add(configured);
        }
        SDK_V1_PROVIDER_MAPPING.forEach(
                (v1Provider, v2Provider) -> {
                    if (!mappedProviders.contains(v1Provider)) {
                        mapping.add(v1Provider + "=" + v2Provider);
                    }
                });
        hadoopConfig.set(Constants.AWS_CREDENTIALS_PROVIDER_MAPPING, mapping.toString());
    }

    private static Map<String, String> createSdkV1ProviderMapping() {
        final Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put(
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider");
        mapping.put(
                "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
                "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider");
        mapping.put(
                "com.amazonaws.auth.SystemPropertiesCredentialsProvider",
                "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider");
        mapping.put(
                "com.amazonaws.auth.ContainerCredentialsProvider",
                "software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider");
        return Collections.unmodifiableMap(mapping);
    }

    @Override
    protected void updateDelegationTokenConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        AbstractS3DelegationTokenReceiver.updateHadoopConfig(
                hadoopConfig, HadoopDynamicTemporaryAWSCredentialsProvider.NAME);
    }
}
