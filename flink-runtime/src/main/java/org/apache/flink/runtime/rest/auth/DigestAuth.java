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

package org.apache.flink.runtime.rest.auth;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Formatter;
import java.util.Locale;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.runtime.rest.auth.CharUtils.isSeparator;
import static org.apache.flink.runtime.rest.auth.CharUtils.isUnsafe;

/** Digest authorization scheme. */
class DigestAuth implements AuthScheme {
    private final String userName;
    private final String password;
    private final MessageDigest digest;
    private final Authenticate challenge;
    private final Optional<String> cnonce;
    private final boolean sess;
    private int nonceCount = 1;

    private static MessageDigest getMessageDigest(final String name) {
        try {
            return MessageDigest.getInstance(name);
        } catch (NoSuchAlgorithmException ex) {
            throw new FlinkRuntimeException(ex);
        }
    }

    private static String encodeHex(final byte[] binary) {
        final StringBuilder sb = new StringBuilder();
        try (final Formatter formatter = new Formatter(sb, Locale.ROOT)) {
            for (byte b : binary) {
                formatter.format("%02x", b & 0xFF);
            }
        }
        return sb.toString();
    }

    protected DigestAuth(
            final String userName, final String password, final Authenticate challenge) {
        final String algorithm = challenge.getAlgorithm().orElse("MD5");
        this.sess = algorithm.endsWith("-sess");
        switch (algorithm) {
            case "MD5-sess":
                /* FALLTHROUGH */
            case "MD5":
                this.digest = getMessageDigest("MD5");
                break;
            case "SHA-256-sess":
                /* FALLTHROUGH */
            case "SHA-256":
                this.digest = getMessageDigest("SHA-256");
                break;
            case "SHA-512-sess":
                /* FALLTHROUGH */
            case "SHA-512":
                this.digest = getMessageDigest("SHA-512");
                break;
            default:
                throw new FlinkRuntimeException("Not supported digest algorithm: " + algorithm);
        }
        this.challenge = challenge;
        this.userName =
                challenge
                        .getUserHash()
                        .map(
                                userHash ->
                                        userHash
                                                ? digest(userName + ":" + challenge.getRealm())
                                                : userName)
                        .orElse(userName);
        this.password = password;
        // To make it simple, we only support qop="auth" at this time
        this.cnonce =
                challenge
                        .getQOP()
                        .map(
                                qop -> {
                                    final SecureRandom rnd = new SecureRandom();
                                    final byte[] tmp = new byte[8];
                                    rnd.nextBytes(tmp);
                                    return encodeHex(tmp);
                                });
    }

    private String digest(final String input) {
        digest.reset();
        return encodeHex(digest.digest(input.getBytes(UTF_8)));
    }

    private String getHA1() {
        String ha1 = digest(userName + ":" + challenge.getRealm() + ":" + password);
        if (sess) {
            ha1 = digest(ha1 + ":" + challenge.getNonce() + ":" + cnonce);
        }
        return ha1;
    }

    private String getHA2(final HttpMethod method, final String uri) {
        // to make it simple, we only support qop="auth"
        return digest(method.name() + ":" + uri);
    }

    /**
     * Returns the digest authorization response for given uri using specified http method.
     *
     * @return digest authorization response for given uri using specified http method
     */
    public String getAuthorization(final HttpMethod method, final String uri) {
        final String ha1 = getHA1();
        final String ha2 = getHA2(method, uri);
        final StringBuilder sb = new StringBuilder(8);
        try (final Formatter formatter = new Formatter(sb, Locale.ROOT)) {
            formatter.format("%08x", nonceCount++);
        }
        final String nc = sb.toString();
        final String response =
                digest(
                        cnonce.map(
                                        c -> {
                                            return ha1
                                                    + ":"
                                                    + challenge.getNonce()
                                                    + ":"
                                                    + nc
                                                    + ":"
                                                    + c
                                                    + ":auth:"
                                                    + ha2;
                                        })
                                .orElseGet(() -> ha1 + ":" + challenge.getNonce() + ":" + ha2));
        sb.setLength(0);
        sb.append("Digest ");
        appendKeyValue(sb, "", "username", userName, true);
        appendKeyValue(sb, ", ", "realm", challenge.getRealm(), true);
        appendKeyValue(sb, ", ", "nonce", challenge.getNonce(), true);
        appendKeyValue(sb, ", ", "uri", uri, false);
        appendKeyValue(sb, ", ", "response", response, true);
        appendKeyValue(sb, ", ", "algorithm", digest.getAlgorithm(), true);
        challenge.getOpaque().map(opaque -> appendKeyValue(sb, ",", "opaque", opaque, true));
        challenge
                .getQOP()
                .map(
                        qop -> {
                            appendKeyValue(sb, ", ", "qop", "auth", false);
                            appendKeyValue(sb, ", ", "nc", nc, false);
                            return appendKeyValue(sb, ", ", "cnonce", cnonce.get(), true);
                        });
        challenge
                .getUserHash()
                .map(
                        userHash ->
                                appendKeyValue(
                                        sb, ", ", "userhash", Boolean.toString(userHash), false));
        return sb.toString();
    }

    /**
     * Returns if current authorization implementation is valid for given authenticate challenge.
     *
     * @return if current authorization implementation is valid for given authenticate challenge
     */
    public boolean isValid(final Authenticate newChallenge) {
        return newChallenge.getScheme().equals(AuthScheme.DIGEST)
                && this.challenge.getRealm().equals(newChallenge.getRealm())
                && this.challenge.getAlgorithm().equals(newChallenge.getAlgorithm())
                && this.challenge.getQOP().equals(newChallenge.getQOP())
                && this.challenge.getNonce().equals(newChallenge.getNonce())
                && this.challenge.getUserHash().equals(newChallenge.getUserHash());
    }

    private static StringBuilder appendKeyValue(
            final StringBuilder sb,
            final String prefix,
            final String key,
            final String value,
            final boolean quote) {
        sb.append(prefix);
        sb.append(key);
        sb.append("=");
        boolean quoteFlag = quote;
        for (int i = 0; (i < value.length()) && !quoteFlag; i++) {
            quoteFlag = isSeparator(value.charAt(i));
        }

        if (quoteFlag) {
            sb.append('"');
        }
        for (int i = 0; i < value.length(); i++) {
            final char ch = value.charAt(i);
            if (isUnsafe(ch)) {
                sb.append('\\');
            }
            sb.append(ch);
        }
        if (quoteFlag) {
            sb.append('"');
        }
        return sb;
    }
}
