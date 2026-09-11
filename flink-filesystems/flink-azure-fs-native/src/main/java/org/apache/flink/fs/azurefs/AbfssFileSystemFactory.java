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

package org.apache.flink.fs.azurefs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.fs.cse.CseOptions;
import org.apache.flink.fs.cse.CseStreamFactory;
import org.apache.flink.fs.cse.KeyProvider;
import org.apache.flink.fs.cse.KeyProviders;
import org.apache.flink.fs.cse.aes.gcm.AesGcmCseStreamFactory;
import org.apache.flink.util.Preconditions;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Factory for Azure Data Lake Storage Gen2 (ADLS Gen2) with the "abfss" scheme.
 *
 * <p>This factory creates {@link AzureDataLakeFileSystem} instances backed by the native Azure SDK.
 * It registers with the "abfss" URI scheme. To select this factory, configure {@code
 * fs.abfss.priority.org.apache.flink.fs.azurefs.AbfssFileSystemFactory: 100} in flink-conf.yaml.
 *
 * <h3>Authentication</h3>
 *
 * <p>Two authentication modes are supported:
 *
 * <ul>
 *   <li><b>SharedKey</b>: when {@code fs.azure.account-key} is configured, SharedKey authentication
 *       is used.
 *   <li><b>DefaultAzureCredential</b>: when no account key is configured, the Azure SDK's {@code
 *       DefaultAzureCredential} chain is used. This auto-detects workload identity, managed
 *       identity, environment variables, and other credential sources. This mode is required in
 *       environments where storage accounts are accessed via managed identity.
 * </ul>
 *
 * <p>This class is NOT thread-safe during configuration, but the configured factory is safe to use
 * from multiple threads.
 *
 * @see AzureFileSystemOptions
 * @see CseOptions
 */
@NotThreadSafe
public class AbfssFileSystemFactory implements FileSystemFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AbfssFileSystemFactory.class);

    @Nullable private Configuration config;

    /**
     * Returns the URI scheme supported by this factory.
     *
     * @return "abfss" for Azure Data Lake Storage Gen2 with SSL
     */
    @Override
    public String getScheme() {
        return "abfss";
    }

    @Override
    public int getPriority() {
        // Deprioritized by default
        return -1;
    }

    @Override
    public void configure(final Configuration config) {
        this.config = config;
    }

    @Override
    public FileSystem create(final URI fsUri) throws IOException {
        final Configuration config = this.config != null ? this.config : new Configuration();

        final String accountName = AzureDataLakeClientProvider.extractAccountNameFromUri(fsUri);

        final MemorySize readBufferMemory = config.get(AzureFileSystemOptions.READ_BUFFER_SIZE);
        final int readBufferSize = Math.toIntExact(readBufferMemory.getBytes());
        if (readBufferSize <= 0) {
            throw new IOException(
                    "Read buffer size must be positive. Got: "
                            + readBufferMemory
                            + ". Configure '"
                            + AzureFileSystemOptions.READ_BUFFER_SIZE.key()
                            + "'.");
        }

        // Account key is optional. When present, SharedKey authentication is used.
        // When absent, the Azure SDK falls back to DefaultAzureCredential which
        // auto-detects workload identity, managed identity, etc. This fallback is
        // required in environments where storage accounts are accessed via managed
        // identity rather than an explicit account key (e.g., HA recovery storage).
        final AzureDataLakeClientProvider.Builder providerBuilder =
                AzureDataLakeClientProvider.builder()
                        .accountName(accountName)
                        .maxRetries(config.get(AzureFileSystemOptions.RETRY_MAX_RETRIES))
                        .baseDelay(config.get(AzureFileSystemOptions.RETRY_BASE_DELAY))
                        .maxDelay(config.get(AzureFileSystemOptions.RETRY_MAX_DELAY))
                        .connectionTimeout(config.get(AzureFileSystemOptions.CONNECTION_TIMEOUT))
                        .readTimeout(config.get(AzureFileSystemOptions.READ_TIMEOUT))
                        .maxIdleConnections(
                                config.get(AzureFileSystemOptions.HTTP_MAX_IDLE_CONNECTIONS))
                        .keepAliveDuration(
                                config.get(AzureFileSystemOptions.HTTP_KEEP_ALIVE_DURATION));

        final String accountKey = config.get(AzureFileSystemOptions.ACCOUNT_KEY);
        if (accountKey != null) {
            providerBuilder.accountKey(accountKey);
        }

        final MemorySize writeRequestMemory = config.get(AzureFileSystemOptions.WRITE_REQUEST_SIZE);
        final long writeRequestSize = writeRequestMemory.getBytes();
        if (writeRequestSize <= 0) {
            throw new IOException(
                    "Write request size must be positive. Got: "
                            + writeRequestMemory
                            + ". Configure '"
                            + AzureFileSystemOptions.WRITE_REQUEST_SIZE.key()
                            + "'.");
        }

        final AzureDataLakeClientProvider clientProvider = providerBuilder.build();
        final String fileSystemName = AzureDataLakeFileSystem.getFileSystemNameFromUri(fsUri);
        final DataLakeFileSystemClient fsClient =
                clientProvider.getFileSystemClient(fileSystemName);

        final String writeKeyId = config.get(CseOptions.WRITE_KEY_ID);
        final CseStreamFactory cseFactory = createCseStreamFactory(config);

        Preconditions.checkArgument(
                cseFactory != null || writeKeyId == null,
                "'%s' is configured but '%s' is absent. CSE writes require both options.",
                CseOptions.WRITE_KEY_ID.key(),
                CseOptions.KEY_PROVIDER_FACTORY_CLASS.key());

        LOG.info(
                "Creating Azure DataLake FileSystem - account: {}, CSE: {}, writeRequestSize: {} bytes",
                accountName,
                cseFactory != null ? (writeKeyId != null ? "read+write" : "read-only") : "disabled",
                writeRequestSize);

        return AzureDataLakeFileSystem.builder(new DataLakeStorageOperationsImpl(fsClient), fsUri)
                .readBufferSize(readBufferSize)
                .writeRequestSize(writeRequestSize)
                .cseFactory(cseFactory)
                .encryptWrites(writeKeyId != null)
                .build();
    }

    /**
     * Creates a {@link CseStreamFactory} if {@link CseOptions#KEY_PROVIDER_FACTORY_CLASS} is
     * configured.
     *
     * @param config the filesystem configuration
     * @return a configured factory, or {@code null} if CSE is disabled
     * @throws IOException if the key provider factory cannot be instantiated
     */
    @Nullable
    private static CseStreamFactory createCseStreamFactory(final Configuration config)
            throws IOException {
        final String factoryClassName = config.get(CseOptions.KEY_PROVIDER_FACTORY_CLASS);
        if (factoryClassName == null) {
            return null;
        }
        // TODO: CseStreamFactory is leaked — FileSystem has no close() lifecycle hook.
        // Fix requires AzureDataLakeFileSystem to implement Closeable (closing the
        // CseStreamFactory, which in turn closes the KeyProvider) and Flink's FileSystem
        // base class to gain a close/dispose lifecycle callback.
        final KeyProvider keyProvider = KeyProviders.instantiate(factoryClassName, config);
        final Map<String, String> encryptionContext = CseOptions.extractEncryptionContext(config);
        final String writeKeyId = config.get(CseOptions.WRITE_KEY_ID);
        return new AesGcmCseStreamFactory(keyProvider, writeKeyId, encryptionContext);
    }
}
