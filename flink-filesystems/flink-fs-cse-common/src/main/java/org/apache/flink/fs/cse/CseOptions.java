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

package org.apache.flink.fs.cse;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Cloud-agnostic client-side encryption (CSE) configuration options shared across all native
 * filesystem plugins.
 *
 * <p>When {@link #KEY_PROVIDER_FACTORY_CLASS} is set, encrypted files are transparently decrypted
 * on read; plaintext files are read as-is. Write encryption is additionally gated on {@link
 * #WRITE_KEY_ID}.
 */
@Experimental
@Immutable
public final class CseOptions {

    /**
     * Prefix shared by all CSE configuration keys. Global (not per-scheme) — all filesystem plugins
     * share the same key provider. Per-scheme overrides can be added later if needed.
     */
    public static final String KEY_PREFIX = "fs.cse.";

    /**
     * Config key prefix for free-form encryption context entries. Keys matching {@code
     * fs.cse.encryption-context.<name>} are extracted into a {@code Map<String, String>} and passed
     * to the {@link KeyProvider} and the CSE factory.
     */
    public static final String ENCRYPTION_CONTEXT_PREFIX = "fs.cse.encryption-context.";

    /**
     * Fully-qualified class name of the {@link KeyProviderFactory} implementation. When absent, CSE
     * is fully disabled.
     */
    public static final ConfigOption<String> KEY_PROVIDER_FACTORY_CLASS =
            ConfigOptions.key("fs.cse.key-provider.factory-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Fully-qualified class name of the KeyProviderFactory implementation. "
                                    + "Must implement org.apache.flink.fs.cse.KeyProviderFactory "
                                    + "with a public no-arg constructor. "
                                    + "When absent, CSE is fully disabled.");

    /**
     * Key identifier used to encrypt new files. Requires {@link #KEY_PROVIDER_FACTORY_CLASS} to be
     * set. When absent, writes are plaintext but encrypted files can still be read.
     */
    public static final ConfigOption<String> WRITE_KEY_ID =
            ConfigOptions.key("fs.cse.write-key-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Key identifier for CSE write encryption. "
                                    + "Requires fs.cse.key-provider.factory-class to be set. "
                                    + "When absent, writes are plaintext.");

    /**
     * Extracts encryption context entries from the configuration by collecting all keys with the
     * {@link #ENCRYPTION_CONTEXT_PREFIX} and stripping the prefix.
     *
     * @param config the filesystem configuration
     * @return an unmodifiable map of encryption context entries (may be empty)
     */
    public static Map<String, String> extractEncryptionContext(final ReadableConfig config) {
        final Map<String, String> context = new HashMap<>();
        for (final Map.Entry<String, String> entry : config.toMap().entrySet()) {
            if (entry.getKey().startsWith(ENCRYPTION_CONTEXT_PREFIX)) {
                context.put(
                        entry.getKey().substring(ENCRYPTION_CONTEXT_PREFIX.length()),
                        entry.getValue());
            }
        }
        return Collections.unmodifiableMap(context);
    }

    private CseOptions() {}
}
