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

/** A collection of utilities about char types. */
public class CharUtils {
    private static final int ARRAY_SIZE = 128;
    private static final boolean[] IS_CONTROL = new boolean[ARRAY_SIZE];
    private static final boolean[] IS_SEPARATOR = new boolean[ARRAY_SIZE];
    private static final boolean[] IS_UNSAFE = new boolean[ARRAY_SIZE];
    private static final boolean[] IS_TOKEN = new boolean[ARRAY_SIZE];
    private static final boolean[] IS_HEX = new boolean[ARRAY_SIZE];
    private static final boolean[] IS_LWS = new boolean[ARRAY_SIZE];

    static {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            // LWS
            if (i == ' ' || i == '\t' || i == '\n' || i == '\r') {
                IS_LWS[i] = true;
            }

            // Control> 0-31, 127
            if (i < 32 || i == 127) {
                IS_CONTROL[i] = true;
                continue;
            }

            // Separator
            if (i == '(' || i == ')' || i == '<' || i == '>' || i == '@' || i == ',' || i == ';'
                    || i == ':' || i == '\\' || i == '\"' || i == '/' || i == '[' || i == ']'
                    || i == '?' || i == '=' || i == '{' || i == '}' || i == ' ' || i == '\t') {
                if (i == '\"' || i == '\\') {
                    IS_UNSAFE[i] = true;
                }
                IS_SEPARATOR[i] = true;
                continue;
            }

            // Token: Anything 0-127 that is not a control and not a separator
            if (i < 128) {
                IS_TOKEN[i] = true;
            }

            // Hex: 0-9, a-f, A-F
            if ((i >= '0' && i <= '9') || (i >= 'a' && i <= 'f') || (i >= 'A' && i <= 'F')) {
                IS_HEX[i] = true;
            }
        }
    }

    /**
     * Returns if given char is a separator.
     *
     * @return if given char is a separator
     */
    public static boolean isSeparator(final int ch) {
        // Fast for correct values, slower for incorrect ones
        try {
            return IS_SEPARATOR[ch];
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
    }

    /**
     * Returns if given char is unsafe and has to been escaped.
     *
     * @return if given char is unsafe and has to be escaped
     */
    public static boolean isUnsafe(final int ch) {
        // Fast for correct values, slower for incorrect ones
        try {
            return IS_UNSAFE[ch];
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
    }

    /**
     * Returns if given char is a token.
     *
     * @return if given char is a token
     */
    public static boolean isToken(final int ch) {
        // Fast for correct values, slower for incorrect ones
        try {
            return IS_TOKEN[ch];
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
    }

    /**
     * Returns if given char is a digit in hex.
     *
     * @return if given char is a digit in hex
     */
    public static boolean isHex(final int ch) {
        // Fast for correct values, slower for incorrect ones
        try {
            return IS_HEX[ch];
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
    }

    /**
     * Returns if given char is a control character.
     *
     * @return if given char is a control character
     */
    public static boolean isControl(final int ch) {
        // Fast for correct values, slower for incorrect ones
        try {
            return IS_CONTROL[ch];
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
    }

    /**
     * Returns if given char is LWS (linear whitespace).
     *
     * @return if given char is LWS (linear whitespace)
     */
    public static boolean isLWS(final int ch) {
        // Fast for correct values, slower for incorrect ones
        try {
            return IS_LWS[ch];
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
    }
}
