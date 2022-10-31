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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.connector.pulsar.testutils.source.cases.ConsumeEncryptMessagesContext;

import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A default key reader for the Pulsar client. We would load the pre-generated key file and validate
 * it.
 */
public class PulsarTestKeyReader implements CryptoKeyReader {
    private static final long serialVersionUID = -7488297938196049791L;

    public static final String DEFAULT_KEY = "flink";
    public static final String DEFAULT_PUBKEY = "/encrypt/test_ecdsa_pubkey.pem";
    public static final String DEFAULT_PRIVKEY = "/encrypt/test_ecdsa_privkey.pem";

    private static final String APPLICATION_X_PEM_FILE = "application/x-pem-file";

    private final String encryptKey;
    private final byte[] publicKey;
    private final byte[] privateKey;

    public PulsarTestKeyReader(String encryptKey, String pubkeyRes, String privkeyRes) {
        this.encryptKey = checkNotNull(encryptKey);
        this.publicKey = loadKey(pubkeyRes);
        this.privateKey = loadKey(privkeyRes);
    }

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
        EncryptionKeyInfo info = new EncryptionKeyInfo();
        if (encryptKey.equals(keyName)) {
            info.setKey(copyKey(publicKey));
        }

        return info;
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
        EncryptionKeyInfo info = new EncryptionKeyInfo();
        if (encryptKey.equals(keyName)) {
            info.setKey(copyKey(privateKey));
        }

        return info;
    }

    private byte[] copyKey(byte[] key) {
        // The byte array is not immutable. Duplicate it for safety.
        byte[] k = new byte[key.length];
        System.arraycopy(key, 0, k, 0, key.length);

        return k;
    }

    private byte[] loadKey(String resourcePath) {
        URL fileURL = ConsumeEncryptMessagesContext.class.getResource(resourcePath);
        checkNotNull(fileURL, "Failed to load resource file: " + resourcePath);

        try {
            URLConnection urlConnection = fileURL.openConnection();
            try {
                String protocol = fileURL.getProtocol();
                String contentType = urlConnection.getContentType();

                if ("data".equals(protocol) && !APPLICATION_X_PEM_FILE.equals(contentType)) {
                    throw new IllegalArgumentException("Unsupported format: " + contentType);
                }
                return IOUtils.toByteArray(urlConnection);
            } finally {
                IOUtils.close(urlConnection);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
