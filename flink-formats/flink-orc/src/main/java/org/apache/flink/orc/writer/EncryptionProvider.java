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

package org.apache.flink.orc.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.KeyProvider;
import org.apache.orc.impl.writer.WriterEncryptionKey;
import org.apache.orc.impl.writer.WriterEncryptionVariant;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Copy encryption variants generation code from org.apache.orc:orc-core:1.7.2 {@link
 * org.apache.orc.impl.WriterImpl}. It's used to get encryption variants which are same as {@link
 * org.apache.orc.impl.WriterImpl} generated.
 *
 * <p>NOTE: This class will be removed after ORC-1200 is merged.
 */
public class EncryptionProvider {

    private final SortedMap<String, WriterEncryptionKey> keys = new TreeMap<>();

    private WriterEncryptionVariant[] encryptionVariants;

    public EncryptionProvider(OrcFile.WriterOptions opts) throws IOException {
        TypeDescription schema = opts.getSchema();
        schema.annotateEncryption(opts.getEncryption(), opts.getMasks());
        this.encryptionVariants =
                setupEncryption(opts.getKeyProvider(), schema, opts.getKeyOverrides());
    }

    /**
     * Iterate through the encryption options given by the user and set up our data structures.
     *
     * @param provider the KeyProvider to use to generate keys
     * @param schema the type tree that we search for annotations
     * @param keyOverrides user specified key overrides
     */
    private WriterEncryptionVariant[] setupEncryption(
            KeyProvider provider,
            TypeDescription schema,
            Map<String, HadoopShims.KeyMetadata> keyOverrides)
            throws IOException {
        KeyProvider keyProvider =
                provider != null
                        ? provider
                        : CryptoUtils.getKeyProvider(new Configuration(), new SecureRandom());
        // Load the overrides into the cache so that we use the required key versions.
        for (HadoopShims.KeyMetadata key : keyOverrides.values()) {
            keys.put(key.getKeyName(), new WriterEncryptionKey(key));
        }
        int variantCount = visitTypeTree(schema, false, keyProvider);

        // Now that we have de-duped the keys and maskDescriptions, make the arrays
        int nextId = 0;
        int nextVariantId = 0;
        WriterEncryptionVariant[] result = new WriterEncryptionVariant[variantCount];
        for (WriterEncryptionKey key : keys.values()) {
            key.setId(nextId++);
            key.sortRoots();
            for (WriterEncryptionVariant variant : key.getEncryptionRoots()) {
                result[nextVariantId] = variant;
                variant.setId(nextVariantId++);
            }
        }
        return result;
    }

    private int visitTypeTree(TypeDescription schema, boolean encrypted, KeyProvider provider)
            throws IOException {
        int result = 0;
        String keyName = schema.getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE);
        if (keyName != null) {
            if (provider == null) {
                throw new IllegalArgumentException("Encryption requires a KeyProvider.");
            }
            if (encrypted) {
                throw new IllegalArgumentException("Nested encryption type: " + schema);
            }
            encrypted = true;
            result += 1;
            WriterEncryptionKey key = getKey(keyName, provider);
            HadoopShims.KeyMetadata metadata = key.getMetadata();
            WriterEncryptionVariant variant =
                    new WriterEncryptionVariant(key, schema, provider.createLocalKey(metadata));
            key.addRoot(variant);
        }
        List<TypeDescription> children = schema.getChildren();
        if (children != null) {
            for (TypeDescription child : children) {
                result += visitTypeTree(child, encrypted, provider);
            }
        }
        return result;
    }

    private WriterEncryptionKey getKey(String keyName, KeyProvider provider) throws IOException {
        WriterEncryptionKey result = keys.get(keyName);
        if (result == null) {
            result = new WriterEncryptionKey(provider.getCurrentKeyVersion(keyName));
            keys.put(keyName, result);
        }
        return result;
    }

    public WriterEncryptionVariant[] getEncryptionVariants() {
        return encryptionVariants;
    }
}
