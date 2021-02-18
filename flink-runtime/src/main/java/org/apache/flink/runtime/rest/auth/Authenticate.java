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

/*
 * copied from org.apache.tomcat.util.http.parser.Authorization (tomcat 10)
 * copied from org.apache.tomcat.util.http.parser.HttpParser (tomcat 10)
 */

package org.apache.flink.runtime.rest.auth;

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.rest.auth.AuthScheme.BASIC;
import static org.apache.flink.runtime.rest.auth.AuthScheme.DIGEST;
import static org.apache.flink.runtime.rest.auth.CharUtils.isLWS;
import static org.apache.flink.runtime.rest.auth.CharUtils.isToken;

/** Authenticate challenge parser. */
public class Authenticate {
    private static final Map<String, FieldType> fieldTypes = new HashMap<>();

    private static final String REALM = "realm";
    private static final String NONCE = "nonce";
    private static final String ALGORITHM = "algorithm";
    private static final String OPAQUE = "opaque";
    private static final String QOP = "qop";
    private static final String STALE = "stale";
    private static final String DOMAIN = "domain";
    private static final String USERHASH = "userhash";

    static {
        // Known authenticate field types.
        // Note: These are more relaxed than RFC2617. This adheres to the
        //       recommendation of RFC2616 that servers are tolerant of buggy
        //       clients when they can be so without ambiguity.
        fieldTypes.put(REALM, FieldType.QUOTED_STRING);
        fieldTypes.put(NONCE, FieldType.QUOTED_STRING);
        // RFC2617 says algorithm is token. <">token<"> will also be accepted
        fieldTypes.put(ALGORITHM, FieldType.QUOTED_TOKEN);
        fieldTypes.put(OPAQUE, FieldType.QUOTED_STRING);
        // RFC2617 says qop is token. <">token<"> will also be accepted
        fieldTypes.put(QOP, FieldType.QUOTED_STRING);
        fieldTypes.put(STALE, FieldType.QUOTED_STRING);
        fieldTypes.put(DOMAIN, FieldType.QUOTED_STRING);
        fieldTypes.put(USERHASH, FieldType.QUOTED_STRING);
    }

    private static Authenticate parse(final StringReader input)
            throws IllegalArgumentException, IOException {
        final Map<String, String> result = new HashMap<>();

        final String scheme;
        if (skipConstant(input, BASIC) == SkipResult.FOUND) {
            scheme = BASIC;
        } else if (skipConstant(input, DIGEST) == SkipResult.FOUND) {
            scheme = DIGEST;
        } else {
            return null;
        }

        // All field names are valid tokens
        String field = readToken(input);
        if (field == null) {
            return null;
        }
        while (!field.equals("")) {
            if (skipConstant(input, "=") != SkipResult.FOUND) {
                return null;
            }
            FieldType type = fieldTypes.get(field.toLowerCase(Locale.ENGLISH));
            if (type == null) {
                // auth-param = token "=" ( token | quoted-string )
                type = FieldType.TOKEN_OR_QUOTED_STRING;
            }
            final String value;
            switch (type) {
                case QUOTED_STRING:
                    value = readQuotedString(input, false);
                    break;
                case TOKEN_OR_QUOTED_STRING:
                    value = readTokenOrQuotedString(input, false);
                    break;
                case QUOTED_TOKEN:
                    value = readQuotedToken(input);
                    break;
                default:
                    value = null;
                    break;
            }

            if (value == null) {
                return null;
            }
            result.put(field, value);

            if (skipConstant(input, ",") == SkipResult.NOT_FOUND) {
                return null;
            }
            field = readToken(input);
            if (field == null) {
                return null;
            }
        }

        return new Authenticate(scheme, result);
    }

    private enum FieldType {
        QUOTED_STRING,
        TOKEN_OR_QUOTED_STRING,
        QUOTED_TOKEN;
    }

    private static String readToken(final Reader input) throws IOException {
        final StringBuilder result = new StringBuilder();

        skipLWS(input);
        input.mark(1);
        int c = input.read();

        while (c != -1 && isToken(c)) {
            result.append((char) c);
            input.mark(1);
            c = input.read();
        }
        // Use mark(1)/reset() rather than skip(-1) since skip() is a NOP
        // once the end of the String has been reached.
        input.reset();

        if (c != -1 && result.length() == 0) {
            return null;
        } else {
            return result.toString();
        }
    }

    private static SkipResult skipConstant(final Reader input, final String constant)
            throws IOException {
        final int len = constant.length();

        skipLWS(input);
        input.mark(len);
        int c = input.read();

        for (int i = 0; i < len; i++) {
            if (i == 0 && c == -1) {
                return SkipResult.EOF;
            }
            if (c != constant.charAt(i)) {
                input.reset();
                return SkipResult.NOT_FOUND;
            }
            if (i != (len - 1)) {
                c = input.read();
            }
        }
        return SkipResult.FOUND;
    }

    /**
     * @return the quoted string if one was found, null if data other than a quoted string was found
     *     or null if the end of data was reached before the quoted string was terminated
     */
    private static String readQuotedString(final Reader input, final boolean returnQuoted)
            throws IOException {

        skipLWS(input);
        int c = input.read();

        if (c != '"') {
            return null;
        }

        final StringBuilder result = new StringBuilder();
        if (returnQuoted) {
            result.append('\"');
        }
        c = input.read();

        while (c != '"') {
            if (c == -1) {
                return null;
            } else if (c == '\\') {
                c = input.read();
                if (returnQuoted) {
                    result.append('\\');
                }
                result.append((char) c);
            } else {
                result.append((char) c);
            }
            c = input.read();
        }
        if (returnQuoted) {
            result.append('\"');
        }

        return result.toString();
    }

    private static String readTokenOrQuotedString(final Reader input, final boolean returnQuoted)
            throws IOException {

        // Peek at next character to enable correct method to be called
        final int c = skipLWS(input);

        if (c == '"') {
            return readQuotedString(input, returnQuoted);
        } else {
            return readToken(input);
        }
    }

    private static String readQuotedToken(final Reader input) throws IOException {
        final StringBuilder result = new StringBuilder();
        boolean quoted = false;

        skipLWS(input);
        input.mark(1);
        int c = input.read();

        if (c == '"') {
            quoted = true;
        } else if (c == -1 || !isToken(c)) {
            return null;
        } else {
            result.append((char) c);
        }
        input.mark(1);
        c = input.read();

        while (c != -1 && isToken(c)) {
            result.append((char) c);
            input.mark(1);
            c = input.read();
        }

        if (quoted) {
            if (c != '"') {
                return null;
            }
        } else {
            // Use mark(1)/reset() rather than skip(-1) since skip() is a NOP
            // once the end of the String has been reached.
            input.reset();
        }

        if (c != -1 && result.length() == 0) {
            return null;
        } else {
            return result.toString();
        }
    }

    // Skip any LWS and position to read the next character. The next character
    // is returned as being able to 'peek()' it allows a small optimisation in
    // some cases.
    private static int skipLWS(final Reader input) throws IOException {
        input.mark(1);
        int c = input.read();

        while (isLWS(c)) {
            input.mark(1);
            c = input.read();
        }

        input.reset();
        return c;
    }

    private enum SkipResult {
        FOUND,
        NOT_FOUND,
        EOF
    }

    private final String scheme;
    private final String realm;
    private final String nonce;
    private final Optional<Boolean> stale;
    private final Optional<String> algorithm;
    private final Optional<String> domain;
    private final Optional<QOPType> qop;
    private final Optional<String> opaque;
    private final Optional<Boolean> userHash;

    private Authenticate(String scheme, Map<String, String> params) {
        this.scheme = scheme;
        this.realm = Preconditions.checkNotNull(params.get(REALM));
        if (AuthScheme.BASIC.equals(scheme)) {
            this.nonce = "";
            this.stale = Optional.empty();
            this.algorithm = Optional.empty();
            this.domain = Optional.empty();
            this.qop = Optional.empty();
            this.opaque = Optional.empty();
            this.userHash = Optional.empty();
            return;
        }
        this.nonce = Preconditions.checkNotNull(params.get(NONCE));
        this.algorithm = Optional.ofNullable(params.get(ALGORITHM));
        this.domain = Optional.ofNullable(params.get(DOMAIN));
        this.opaque = Optional.ofNullable(params.get(OPAQUE));
        this.qop = Optional.ofNullable(params.get(QOP)).flatMap(Authenticate::parseQOP);
        this.stale = Optional.ofNullable(params.get(STALE)).map(Boolean::parseBoolean);
        this.userHash = Optional.ofNullable(params.get(USERHASH)).map(Boolean::parseBoolean);
    }

    /**
     * Returns the scheme of authenticate challenge.
     *
     * @return authenticate scheme
     */
    public String getScheme() {
        return scheme;
    }

    /**
     * Returns the nonce of authenticate challenge.
     *
     * @return challenge nonce
     */
    public String getNonce() {
        return nonce;
    }

    /**
     * Returns the realm of authenticate challenge.
     *
     * @return challenge realm
     */
    public String getRealm() {
        return realm;
    }

    /**
     * Returns if authorization response is stale.
     *
     * @return if response is stale
     */
    public Optional<Boolean> getStale() {
        return stale;
    }

    /**
     * Returns the algorithm of authenticate challenge.
     *
     * @return challenge algorithm
     */
    public Optional<String> getAlgorithm() {
        return algorithm;
    }

    /**
     * Returns the domain of authenticate challenge.
     *
     * @return challenge domain
     */
    public Optional<String> getDomain() {
        return domain;
    }

    /**
     * Returns the qop of authenticate challenge.
     *
     * @return challenge qop
     */
    public Optional<QOPType> getQOP() {
        return qop;
    }

    /**
     * Returns the opaque data of authenticate challenge.
     *
     * @return challenge opaque
     */
    public Optional<String> getOpaque() {
        return opaque;
    }

    /**
     * Returns if client should use user hash in response.
     *
     * @return if client should use user hash
     */
    public Optional<Boolean> getUserHash() {
        return userHash;
    }

    /** Parse a authenticate challenge from header. */
    public static Authenticate parse(final String authenticate) {
        try {
            final Authenticate result = parse(new StringReader(authenticate));
            if (result == null) {
                throw new ParseException(
                        String.format("Failed to parse authenticate challenge: %s", authenticate));
            }
            return result;
        } catch (IOException | IllegalArgumentException throwable) {
            throw new ParseException(
                    String.format("Failed to parse authenticate challenge: %s", authenticate),
                    throwable);
        }
    }

    /** Digest challenge qop type. */
    public enum QOPType {
        AUTH,
        AUTH_INT,
    }

    private static Optional<QOPType> parseQOP(final String s) {
        final String[] candidates = s.toLowerCase().split(",");
        for (String candidate : candidates) {
            if ("auth".equals(s)) {
                // Only auth is supported at this time
                return Optional.of(QOPType.AUTH);
            }
        }
        return Optional.empty();
    }
}
