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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * General utilities for string-encoding. This class is used to avoid additional dependencies to
 * other projects.
 */
@Internal
public abstract class EncodingUtils {

    private static final Base64.Encoder BASE64_ENCODER =
            java.util.Base64.getUrlEncoder().withoutPadding();

    private static final Base64.Decoder BASE64_DECODER = java.util.Base64.getUrlDecoder();

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private EncodingUtils() {
        // do not instantiate
    }

    public static String escapeBackticks(String s) {
        return s.replace("`", "``");
    }

    public static String escapeSingleQuotes(String s) {
        return s.replace("'", "''");
    }

    public static String escapeIdentifier(String s) {
        return "`" + escapeBackticks(s) + "`";
    }

    public static String encodeObjectToString(Object serializable) {
        try {
            final byte[] bytes = InstantiationUtil.serializeObject(serializable);
            return new String(BASE64_ENCODER.encode(bytes), UTF_8);
        } catch (Exception e) {
            throw new ValidationException(
                    "Unable to serialize object '"
                            + serializable.toString()
                            + "' of class '"
                            + serializable.getClass().getName()
                            + "'.",
                    e);
        }
    }

    public static String encodeObjectToString(Serializable obj) {
        return encodeObjectToString((Object) obj);
    }

    public static <T> T decodeStringToObject(String encodedStr, ClassLoader classLoader)
            throws IOException {
        final byte[] bytes = BASE64_DECODER.decode(encodedStr.getBytes(UTF_8));
        try {
            return InstantiationUtil.deserializeObject(bytes, classLoader);
        } catch (ClassNotFoundException e) {
            throw new ValidationException(
                    "Unable to deserialize string '" + encodedStr + "' to object.", e);
        }
    }

    public static <T extends Serializable> T decodeStringToObject(
            String base64String, Class<T> baseClass) {
        return decodeStringToObject(
                base64String, baseClass, Thread.currentThread().getContextClassLoader());
    }

    public static <T extends Serializable> T decodeStringToObject(
            String base64String, Class<T> baseClass, ClassLoader classLoader) {
        try {
            final byte[] bytes = BASE64_DECODER.decode(base64String.getBytes(UTF_8));
            final T instance = InstantiationUtil.deserializeObject(bytes, classLoader);
            if (instance != null && !baseClass.isAssignableFrom(instance.getClass())) {
                throw new ValidationException(
                        "Object '"
                                + instance
                                + "' does not match expected base class '"
                                + baseClass
                                + "' but is of class '"
                                + instance.getClass()
                                + "'.");
            }
            return instance;
        } catch (Exception e) {
            throw new ValidationException(
                    "Unable to deserialize string '"
                            + base64String
                            + "' of base class '"
                            + baseClass.getName()
                            + "'.",
                    e);
        }
    }

    public static Class<?> loadClass(String qualifiedName, ClassLoader classLoader) {
        try {
            return Class.forName(qualifiedName, true, classLoader);
        } catch (Exception e) {
            throw new ValidationException(
                    "Class '"
                            + qualifiedName
                            + "' could not be loaded. "
                            + "Please note that inner classes must be globally accessible and declared static.",
                    e);
        }
    }

    public static Class<?> loadClass(String qualifiedName) {
        return loadClass(qualifiedName, Thread.currentThread().getContextClassLoader());
    }

    public static String encodeBytesToBase64(byte[] bytes) {
        return new String(java.util.Base64.getEncoder().encode(bytes), UTF_8);
    }

    public static byte[] decodeBase64ToBytes(String base64) {
        return java.util.Base64.getDecoder().decode(base64.getBytes(UTF_8));
    }

    public static String encodeStringToBase64(String string) {
        return encodeBytesToBase64(string.getBytes(UTF_8));
    }

    public static String decodeBase64ToString(String base64) {
        return new String(decodeBase64ToBytes(base64), UTF_8);
    }

    public static byte[] md5(String string) {
        try {
            return MessageDigest.getInstance("MD5").digest(string.getBytes(UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new TableException("Unsupported MD5 algorithm.", e);
        }
    }

    public static String hex(String string) {
        return hex(string.getBytes(UTF_8));
    }

    public static String hex(byte[] bytes) {
        // adopted from https://stackoverflow.com/a/9855338
        final char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            final int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_CHARS[v >>> 4];
            hexChars[j * 2 + 1] = HEX_CHARS[v & 0x0F];
        }
        return new String(hexChars);
    }

    // --------------------------------------------------------------------------------------------
    // Java String Repetition
    //
    // copied from o.a.commons.lang3.StringUtils (commons-lang3:3.3.2)
    // --------------------------------------------------------------------------------------------

    private static final String EMPTY = "";

    /** The maximum size to which the padding constant(s) can expand. */
    private static final int PAD_LIMIT = 8192;

    /**
     * Repeat a String {@code repeat} times to form a new String.
     *
     * <pre>
     * StringUtils.repeat(null, 2) = null
     * StringUtils.repeat("", 0)   = ""
     * StringUtils.repeat("", 2)   = ""
     * StringUtils.repeat("a", 3)  = "aaa"
     * StringUtils.repeat("ab", 2) = "abab"
     * StringUtils.repeat("a", -2) = ""
     * </pre>
     *
     * @param str the String to repeat, may be null
     * @param repeat number of times to repeat str, negative treated as zero
     * @return a new String consisting of the original String repeated, {@code null} if null String
     *     input
     */
    public static String repeat(final String str, final int repeat) {
        // Performance tuned for 2.0 (JDK1.4)

        if (str == null) {
            return null;
        }
        if (repeat <= 0) {
            return EMPTY;
        }
        final int inputLength = str.length();
        if (repeat == 1 || inputLength == 0) {
            return str;
        }
        if (inputLength == 1 && repeat <= PAD_LIMIT) {
            return repeat(str.charAt(0), repeat);
        }

        final int outputLength = inputLength * repeat;
        switch (inputLength) {
            case 1:
                return repeat(str.charAt(0), repeat);
            case 2:
                final char ch0 = str.charAt(0);
                final char ch1 = str.charAt(1);
                final char[] output2 = new char[outputLength];
                for (int i = repeat * 2 - 2; i >= 0; i--, i--) {
                    output2[i] = ch0;
                    output2[i + 1] = ch1;
                }
                return new String(output2);
            default:
                final StringBuilder buf = new StringBuilder(outputLength);
                for (int i = 0; i < repeat; i++) {
                    buf.append(str);
                }
                return buf.toString();
        }
    }

    /**
     * Returns padding using the specified delimiter repeated to a given length.
     *
     * <pre>
     * StringUtils.repeat('e', 0)  = ""
     * StringUtils.repeat('e', 3)  = "eee"
     * StringUtils.repeat('e', -2) = ""
     * </pre>
     *
     * <p>Note: this method doesn't not support padding with <a
     * href="http://www.unicode.org/glossary/#supplementary_character">Unicode Supplementary
     * Characters</a> as they require a pair of {@code char}s to be represented. If you are needing
     * to support full I18N of your applications consider using {@link #repeat(String, int)}
     * instead.
     *
     * @param ch character to repeat
     * @param repeat number of times to repeat char, negative treated as zero
     * @return String with repeated character
     * @see #repeat(String, int)
     */
    public static String repeat(final char ch, final int repeat) {
        final char[] buf = new char[repeat];
        for (int i = repeat - 1; i >= 0; i--) {
            buf[i] = ch;
        }
        return new String(buf);
    }

    // --------------------------------------------------------------------------------------------
    // Java String Escaping
    //
    // copied from o.a.commons.lang.StringEscapeUtils (commons-lang:2.4)
    // but without escaping forward slashes.
    // --------------------------------------------------------------------------------------------

    /**
     * Escapes the characters in a <code>String</code> using Java String rules.
     *
     * <p>Deals correctly with quotes and control-chars (tab, backslash, cr, ff, etc.)
     *
     * <p>So a tab becomes the characters <code>'\\'</code> and <code>'t'</code>.
     *
     * <p>The only difference between Java strings and JavaScript strings is that in JavaScript, a
     * single quote must be escaped.
     *
     * <p>Example:
     *
     * <pre>
     * input string: He didn't say, "Stop!"
     * output string: He didn't say, \"Stop!\"
     * </pre>
     *
     * @param str String to escape values in, may be null
     * @return String with escaped values, <code>null</code> if null string input
     */
    public static String escapeJava(String str) {
        return escapeJavaStyleString(str, false);
    }

    private static String escapeJavaStyleString(String str, boolean escapeSingleQuotes) {
        if (str == null) {
            return null;
        }
        try {
            StringWriter writer = new StringWriter(str.length() * 2);
            escapeJavaStyleString(writer, str, escapeSingleQuotes);
            return writer.toString();
        } catch (IOException ioe) {
            // this should never ever happen while writing to a StringWriter
            ioe.printStackTrace();
            return null;
        }
    }

    private static void escapeJavaStyleString(Writer out, String str, boolean escapeSingleQuote)
            throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("The Writer must not be null");
        }
        if (str == null) {
            return;
        }
        int sz;
        sz = str.length();
        for (int i = 0; i < sz; i++) {
            char ch = str.charAt(i);

            // handle unicode
            if (ch > 0xfff) {
                out.write("\\u" + hex(ch));
            } else if (ch > 0xff) {
                out.write("\\u0" + hex(ch));
            } else if (ch > 0x7f) {
                out.write("\\u00" + hex(ch));
            } else if (ch < 32) {
                switch (ch) {
                    case '\b':
                        out.write('\\');
                        out.write('b');
                        break;
                    case '\n':
                        out.write('\\');
                        out.write('n');
                        break;
                    case '\t':
                        out.write('\\');
                        out.write('t');
                        break;
                    case '\f':
                        out.write('\\');
                        out.write('f');
                        break;
                    case '\r':
                        out.write('\\');
                        out.write('r');
                        break;
                    default:
                        if (ch > 0xf) {
                            out.write("\\u00" + hex(ch));
                        } else {
                            out.write("\\u000" + hex(ch));
                        }
                        break;
                }
            } else {
                switch (ch) {
                    case '\'':
                        if (escapeSingleQuote) {
                            out.write('\\');
                        }
                        out.write('\'');
                        break;
                    case '"':
                        out.write('\\');
                        out.write('"');
                        break;
                    case '\\':
                        out.write('\\');
                        out.write('\\');
                        break;
                        // MODIFICATION: Flink removes invalid escaping of forward slashes!
                        // case '/':
                        //	out.write('\\');
                        //	out.write('/');
                        //	break;
                    default:
                        out.write(ch);
                        break;
                }
            }
        }
    }

    private static String hex(char ch) {
        return Integer.toHexString(ch).toUpperCase();
    }
}
