/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.runtime;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.CartesianProductEnumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.TimeFrame;
import org.apache.calcite.rel.type.TimeFrameSet;
import org.apache.calcite.runtime.FlatLists.ComparableList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.TimeWithTimeZoneString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.Unsafe;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.format.FormatElement;
import org.apache.calcite.util.format.FormatModel;
import org.apache.calcite.util.format.FormatModels;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Normalizer;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.calcite.config.CalciteSystemProperty.FUNCTION_LEVEL_CACHE_MAX_SIZE;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Flink modifications:
 *
 * <p>so far there are no Flink modifications. Could be removed once upgraded to the version with
 * fixed CALCITE-6393.
 */
@SuppressWarnings("UnnecessaryUnboxing")
@Deterministic
public class SqlFunctions {
    private static final String COMMA_DELIMITER = ",";

    @SuppressWarnings("unused")
    private static final DecimalFormat DOUBLE_FORMAT = NumberUtil.decimalFormat("0.0E0");

    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    private static final DateTimeFormatter ROOT_DAY_FORMAT =
            DateTimeFormatter.ofPattern("EEEE", Locale.ROOT);

    private static final DateTimeFormatter ROOT_MONTH_FORMAT =
            DateTimeFormatter.ofPattern("MMMM", Locale.ROOT);

    private static final Soundex SOUNDEX = new Soundex();

    private static final int SOUNDEX_LENGTH = 4;

    private static final LevenshteinDistance LEVENSHTEIN_DISTANCE =
            LevenshteinDistance.getDefaultInstance();

    private static final Pattern FROM_BASE64_REGEXP = Pattern.compile("[\\t\\n\\r\\s]");

    private static final Base32 BASE_32 = new Base32();

    // Some JVMs can't allocate arrays of length Integer.MAX_VALUE; actual max is somewhat smaller.
    // Be conservative and lower this value a little.
    // @see
    // http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/tip/src/share/classes/java/util/ArrayList.java#l229
    // Note: this variable handling is inspired by Apache Spark
    private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 15;

    private static final Function1<List<Object>, Enumerable<Object>> LIST_AS_ENUMERABLE =
            a0 -> a0 == null ? Linq4j.emptyEnumerable() : Linq4j.asEnumerable(a0);

    @SuppressWarnings("unused")
    private static final Function1<Object[], Enumerable<Object[]>> ARRAY_CARTESIAN_PRODUCT =
            lists -> {
                final List<Enumerator<Object>> enumerators = new ArrayList<>();
                for (Object list : lists) {
                    enumerators.add(Linq4j.enumerator((List) list));
                }
                final Enumerator<List<Object>> product = Linq4j.product(enumerators);
                return new AbstractEnumerable<Object[]>() {
                    @Override
                    public Enumerator<Object[]> enumerator() {
                        return Linq4j.transform(product, List::toArray);
                    }
                };
            };

    /**
     * Holds, for each thread, a map from sequence name to sequence current value.
     *
     * <p>This is a straw man of an implementation whose main goal is to prove that sequences can be
     * parsed, validated and planned. A real application will want persistent values for sequences,
     * shared among threads.
     */
    private static final ThreadLocal<Map<String, AtomicLong>> THREAD_SEQUENCES =
            ThreadLocal.withInitial(HashMap::new);

    private static final Pattern PATTERN_0_STAR_E = Pattern.compile("0*E");

    /** A byte string consisting of a single byte that is the ASCII space character (0x20). */
    private static final ByteString SINGLE_SPACE_BYTE_STRING = ByteString.of("20", 16);

    // Date formatter for BigQuery's timestamp literals:
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#timestamp_literals
    private static final DateTimeFormatter BIG_QUERY_TIMESTAMP_LITERAL_FORMATTER =
            new DateTimeFormatterBuilder()
                    // Unlike ISO 8601, BQ only supports years between 1 - 9999,
                    // but can support single-digit month and day parts.
                    .appendValue(ChronoField.YEAR, 4)
                    .appendLiteral('-')
                    .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
                    .appendLiteral('-')
                    .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                    // Everything after the date is optional. Optional sections can be nested.
                    .optionalStart()
                    // BQ accepts either a literal 'T' or a space to separate the date from the
                    // time,
                    // so make the 'T' optional but pad with 1 space if it's omitted.
                    .padNext(1, ' ')
                    .optionalStart()
                    .appendLiteral('T')
                    .optionalEnd()
                    // Unlike ISO 8601, BQ can support single-digit hour, minute, and second parts.
                    .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
                    .appendLiteral(':')
                    .appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
                    .appendLiteral(':')
                    .appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
                    // ISO 8601 supports up to nanosecond precision, but BQ only up to microsecond.
                    .optionalStart()
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .optionalEnd()
                    .optionalStart()
                    .parseLenient()
                    .appendOffsetId()
                    .toFormatter(Locale.ROOT);

    /** Whether the current Java version is 8 (1.8). */
    private static final boolean IS_JDK_8 = System.getProperty("java.version").startsWith("1.8");

    private SqlFunctions() {}

    /**
     * Internal THROW_UNLESS(condition, message) function.
     *
     * <p>The method is marked {@link NonDeterministic} to prevent the generator from storing its
     * value as a constant.
     */
    @NonDeterministic
    public static boolean throwUnless(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
        return condition;
    }

    /** SQL TO_BASE64(string) function. */
    public static String toBase64(String string) {
        return toBase64_(string.getBytes(UTF_8));
    }

    /** SQL TO_BASE64(string) function for binary string. */
    public static String toBase64(ByteString string) {
        return toBase64_(string.getBytes());
    }

    private static String toBase64_(byte[] bytes) {
        String base64 = Base64.getEncoder().encodeToString(bytes);
        StringBuilder str = new StringBuilder(base64.length() + base64.length() / 76);
        Splitter.fixedLength(76)
                .split(base64)
                .iterator()
                .forEachRemaining(
                        s -> {
                            str.append(s);
                            str.append("\n");
                        });
        return str.substring(0, str.length() - 1);
    }

    /** SQL FROM_BASE64(string) function. */
    public static @Nullable ByteString fromBase64(String base64) {
        try {
            base64 = FROM_BASE64_REGEXP.matcher(base64).replaceAll("");
            return new ByteString(Base64.getDecoder().decode(base64));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** SQL TO_BASE32(string) function. */
    public static String toBase32(String string) {
        return toBase32_(string.getBytes(UTF_8));
    }

    /** SQL TO_BASE32(string) function for binary string. */
    public static String toBase32(ByteString string) {
        return toBase32_(string.getBytes());
    }

    private static String toBase32_(byte[] bytes) {
        return BASE_32.encodeToString(bytes);
    }

    /** SQL FROM_BASE32(string) function. */
    public static ByteString fromBase32(String base32) {
        return new ByteString(BASE_32.decode(base32));
    }

    /** SQL FROM_HEX(varchar) function. */
    public static ByteString fromHex(String hex) {
        try {
            return new ByteString(Hex.decodeHex(hex));
        } catch (DecoderException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Failed to decode hex string: %s", hex), e);
        }
    }

    /** SQL TO_HEX(binary) function. */
    public static String toHex(ByteString byteString) {
        return Hex.encodeHexString(byteString.getBytes());
    }

    /** SQL MD5(string) function. */
    public static String md5(String string) {
        return DigestUtils.md5Hex(string.getBytes(UTF_8));
    }

    /** SQL MD5(string) function for binary string. */
    public static String md5(ByteString string) {
        return DigestUtils.md5Hex(string.getBytes());
    }

    /** SQL SHA1(string) function. */
    public static String sha1(String string) {
        return DigestUtils.sha1Hex(string.getBytes(UTF_8));
    }

    /** SQL SHA1(string) function for binary string. */
    public static String sha1(ByteString string) {
        return DigestUtils.sha1Hex(string.getBytes());
    }

    /** SQL SHA256(string) function. */
    public static String sha256(String string) {
        return DigestUtils.sha256Hex(string.getBytes(UTF_8));
    }

    /** SQL SHA256(string) function for binary string. */
    public static String sha256(ByteString string) {
        return DigestUtils.sha256Hex(string.getBytes());
    }

    /** SQL SHA512(string) function. */
    public static String sha512(String string) {
        return DigestUtils.sha512Hex(string.getBytes(UTF_8));
    }

    /** SQL SHA512(string) function for binary string. */
    public static String sha512(ByteString string) {
        return DigestUtils.sha512Hex(string.getBytes());
    }

    /**
     * State for {@code REGEXP_CONTAINS}, {@code REGEXP_EXTRACT}, {@code REGEXP_EXTRACT_ALL}, {@code
     * REGEXP_INSTR}, {@code REGEXP_REPLACE}, {@code RLIKE}.
     *
     * <p>Marked deterministic so that the code generator instantiates one once per query, not once
     * per row.
     */
    @Deterministic
    public static class RegexFunction {
        /** Cache key. */
        private static class Key extends Ord<String> {
            Key(int flags, String regex) {
                super(flags, regex);
            }

            @SuppressWarnings("MagicConstant")
            Pattern toPattern() {
                return Pattern.compile(e, i);
            }
        }

        private final LoadingCache<Key, Pattern> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(CacheLoader.from(Key::toPattern));

        private final LoadingCache<String, String> replacementStrCache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(CacheLoader.from(RegexFunction::replaceNonDollarIndexedString));

        /**
         * Validate regex arguments in REGEXP_* fns, throws an exception for invalid regex patterns,
         * else returns a Pattern object.
         */
        private Pattern validateRegexPattern(String regex, String methodName) {
            try {
                // Uses java.util.regex as a standard for regex processing
                // in Calcite instead of RE2 used by BigQuery/GoogleSQL
                return cache.getUnchecked(new Key(0, regex));
            } catch (UncheckedExecutionException e) {
                if (e.getCause() instanceof PatternSyntaxException) {
                    throw RESOURCE.invalidRegexInputForRegexpFunctions(
                                    requireNonNull(e.getCause().getMessage(), "message")
                                            .replace(System.lineSeparator(), " "),
                                    methodName)
                            .ex();
                }
                throw e;
            }
        }

        /**
         * Checks for multiple capturing groups in regex arguments in REGEXP_* functions. Throws if
         * the regex pattern has more than 1 capturing group.
         */
        private static void checkMultipleCapturingGroupsInRegex(
                Matcher matcher, String methodName) {
            if (matcher.groupCount() > 1) {
                throw RESOURCE.multipleCapturingGroupsForRegexpFunctions(
                                Integer.toString(matcher.groupCount()), methodName)
                        .ex();
            }
        }

        /**
         * Validates the value ranges for position and occurrence arguments in REGEXP_* functions.
         * Functions not using the {@code occurrencePosition} parameter pass a default value of 0.
         * Throws an exception or returns false if any arguments are beyond accepted range; returns
         * true if all argument values are valid.
         */
        private static boolean validatePosOccurrenceParamValues(
                int position,
                int occurrence,
                int occurrencePosition,
                String value,
                String methodName) {
            if (position <= 0) {
                throw RESOURCE.invalidIntegerInputForRegexpFunctions(
                                Integer.toString(position), "position", methodName)
                        .ex();
            }
            if (occurrence <= 0) {
                throw RESOURCE.invalidIntegerInputForRegexpFunctions(
                                Integer.toString(occurrence), "occurrence", methodName)
                        .ex();
            }
            if (occurrencePosition != 0 && occurrencePosition != 1) {
                throw RESOURCE.invalidIntegerInputForRegexpFunctions(
                                Integer.toString(occurrencePosition),
                                "occurrence_position",
                                methodName)
                        .ex();
            }
            return position <= value.length();
        }

        /**
         * Preprocesses double-backslash-based indexing for capturing groups into $-based indices
         * recognized by java regex, throws an error for invalid escapes.
         */
        public static String replaceNonDollarIndexedString(String replacement) {
            // Explicitly escaping any $ symbols coming from input
            // to ignore them from being considered as capturing group index
            String indexedReplacement = replacement.replace("\\\\", "\\").replace("$", "\\$");

            // Check each occurrence of escaped chars, convert '\<n>' integers into '$<n>' indices,
            // keep occurrences of '\\' and '\$', throw an error for any other invalid escapes
            int lastOccIdx = indexedReplacement.indexOf("\\");
            while (lastOccIdx != -1 && lastOccIdx < indexedReplacement.length() - 1) {
                // Fetch escaped symbol following the current '\' occurrence
                final char escapedChar = indexedReplacement.charAt(lastOccIdx + 1);

                // Replace '\<n>' with '$<n>' if escaped char is an integer
                if (Character.isDigit(escapedChar)) {
                    indexedReplacement = indexedReplacement.replaceFirst("\\\\(\\d)", "\\$$1");
                } else if (escapedChar != '\\' && escapedChar != '$') {
                    // Throw an error if escaped char is not an escaped '\' or an escaped '$'
                    throw RESOURCE.invalidReplacePatternForRegexpReplace(replacement).ex();
                }
                // Fetch next occurrence index after current escaped char
                lastOccIdx = indexedReplacement.indexOf("\\", lastOccIdx + 2);
            }

            return indexedReplacement;
        }

        /**
         * SQL {@code REGEXP_CONTAINS(value, regexp)} function. Throws a runtime exception for
         * invalid regular expressions.
         */
        public boolean regexpContains(String value, String regex) {
            final Pattern pattern = validateRegexPattern(regex, "REGEXP_CONTAINS");
            return pattern.matcher(value).find();
        }

        /**
         * SQL {@code REGEXP_EXTRACT(value, regexp)} function. Returns NULL if there is no match.
         * Returns an exception if regex is invalid. Uses position=1 and occurrence=1 as default
         * values when not specified.
         */
        public @Nullable String regexpExtract(String value, String regex) {
            return regexpExtract(value, regex, 1, 1);
        }

        /**
         * SQL {@code REGEXP_EXTRACT(value, regexp, position)} function. Returns NULL if there is no
         * match, or if position is beyond range. Returns an exception if regex or position is
         * invalid. Uses occurrence=1 as default value when not specified.
         */
        public @Nullable String regexpExtract(String value, String regex, int position) {
            return regexpExtract(value, regex, position, 1);
        }

        /**
         * SQL {@code REGEXP_EXTRACT(value, regexp, position, occurrence)} function. Returns NULL if
         * there is no match, or if position or occurrence are beyond range. Returns an exception if
         * regex, position or occurrence are invalid.
         */
        public @Nullable String regexpExtract(
                String value, String regex, int position, int occurrence) {
            // Uses java.util.regex as a standard for regex processing
            // in Calcite instead of RE2 used by BigQuery/GoogleSQL
            final String methodName = "REGEXP_EXTRACT";
            final Pattern pattern = validateRegexPattern(regex, methodName);

            if (!validatePosOccurrenceParamValues(position, occurrence, 0, value, methodName)) {
                return null;
            }

            Matcher matcher = pattern.matcher(value);
            checkMultipleCapturingGroupsInRegex(matcher, methodName);
            matcher.region(position - 1, value.length());

            String match = null;
            while (occurrence > 0) {
                if (matcher.find()) {
                    match = matcher.group(matcher.groupCount());
                } else {
                    return null;
                }
                occurrence--;
            }

            return match;
        }

        /**
         * SQL {@code REGEXP_EXTRACT_ALL(value, regexp)} function. Returns an empty array if there
         * is no match, returns an exception if regex is invalid.
         */
        public List<String> regexpExtractAll(String value, String regex) {
            // Uses java.util.regex as a standard for regex processing
            // in Calcite instead of RE2 used by BigQuery/GoogleSQL
            final String methodName = "REGEXP_EXTRACT_ALL";
            final Pattern regexp = validateRegexPattern(regex, methodName);

            Matcher matcher = regexp.matcher(value);
            checkMultipleCapturingGroupsInRegex(matcher, methodName);

            ImmutableList.Builder<String> matches = ImmutableList.builder();
            while (matcher.find()) {
                String match = matcher.group(matcher.groupCount());
                if (match != null) {
                    matches.add(match);
                }
            }
            return matches.build();
        }

        /**
         * SQL {@code REGEXP_INSTR(value, regexp)} function. Returns 0 if there is no match or regex
         * is empty. Returns an exception if regex is invalid. Uses position=1, occurrence=1,
         * occurrencePosition=0 as default values if not specified.
         */
        public int regexpInstr(String value, String regex) {
            return regexpInstr(value, regex, 1, 1, 0);
        }

        /**
         * SQL {@code REGEXP_INSTR(value, regexp, position)} function. Returns 0 if there is no
         * match, regex is empty, or if position is beyond range. Returns an exception if regex or
         * position is invalid. Uses occurrence=1, occurrencePosition=0 as default value when not
         * specified.
         */
        public int regexpInstr(String value, String regex, int position) {
            return regexpInstr(value, regex, position, 1, 0);
        }

        /**
         * SQL {@code REGEXP_INSTR(value, regexp, position, occurrence)} function. Returns 0 if
         * there is no match, regex is empty, or if position or occurrence are beyond range. Returns
         * an exception if regex, position or occurrence are invalid. Uses occurrencePosition=0 as
         * default value when not specified.
         */
        public int regexpInstr(String value, String regex, int position, int occurrence) {
            return regexpInstr(value, regex, position, occurrence, 0);
        }

        /**
         * SQL {@code REGEXP_INSTR(value, regexp, position, occurrence, occurrencePosition)}
         * function. Returns 0 if there is no match, regex is empty, or if position or occurrence
         * are beyond range. Returns an exception if regex, position, occurrence or
         * occurrencePosition are invalid.
         */
        public int regexpInstr(
                String value, String regex, int position, int occurrence, int occurrencePosition) {
            // Uses java.util.regex as a standard for regex processing
            // in Calcite instead of RE2 used by BigQuery/GoogleSQL
            final String methodName = "REGEXP_INSTR";
            final Pattern pattern = validateRegexPattern(regex, methodName);

            if (regex.isEmpty()
                    || !validatePosOccurrenceParamValues(
                            position, occurrence, occurrencePosition, value, methodName)) {
                return 0;
            }

            Matcher matcher = pattern.matcher(value);
            checkMultipleCapturingGroupsInRegex(matcher, methodName);
            matcher.region(position - 1, value.length());

            int matchIndex = 0;
            while (occurrence > 0) {
                if (matcher.find()) {
                    if (occurrencePosition == 0) {
                        matchIndex = matcher.start(matcher.groupCount()) + 1;
                    } else {
                        matchIndex = matcher.end(matcher.groupCount()) + 1;
                    }
                } else {
                    return 0;
                }
                occurrence--;
            }

            return matchIndex;
        }

        /** SQL {@code REGEXP_REPLACE} function with 3 arguments. */
        public String regexpReplace(String s, String regex, String replacement) {
            return regexpReplace(s, regex, replacement, 1, 0, null);
        }

        /** SQL {@code REGEXP_REPLACE} function with 4 arguments. */
        public String regexpReplace(String s, String regex, String replacement, int pos) {
            return regexpReplace(s, regex, replacement, pos, 0, null);
        }

        /** SQL {@code REGEXP_REPLACE} function with 5 arguments. */
        public String regexpReplace(
                String s, String regex, String replacement, int pos, int occurrence) {
            return regexpReplace(s, regex, replacement, pos, occurrence, null);
        }

        /** SQL {@code REGEXP_REPLACE} function with 6 arguments. */
        public String regexpReplace(
                String s,
                String regex,
                String replacement,
                int pos,
                int occurrence,
                @Nullable String matchType) {
            if (pos < 1 || pos > s.length()) {
                throw RESOURCE.invalidInputForRegexpReplace(Integer.toString(pos)).ex();
            }

            final int flags = matchType == null ? 0 : makeRegexpFlags(matchType);
            final Pattern pattern = cache.getUnchecked(new Key(flags, regex));

            return Unsafe.regexpReplace(s, pattern, replacement, pos, occurrence);
        }

        /**
         * SQL {@code REGEXP_REPLACE} function with 3 arguments with {@code \\} based indexing for
         * capturing groups.
         */
        public String regexpReplaceNonDollarIndexed(String s, String regex, String replacement) {
            // Modify double-backslash capturing group indices in replacement argument,
            // retrieved from cache when available.
            String indexedReplacement;
            try {
                indexedReplacement = replacementStrCache.getUnchecked(replacement);
            } catch (UncheckedExecutionException e) {
                if (e.getCause() instanceof CalciteException) {
                    throw RESOURCE.invalidReplacePatternForRegexpReplace(replacement).ex();
                }
                throw e;
            }

            // Call generic regexp replace method with modified replacement pattern
            return regexpReplace(s, regex, indexedReplacement, 1, 0, null);
        }

        private static int makeRegexpFlags(String stringFlags) {
            int flags = 0;
            for (int i = 0; i < stringFlags.length(); ++i) {
                switch (stringFlags.charAt(i)) {
                    case 'i':
                        flags |= Pattern.CASE_INSENSITIVE;
                        break;
                    case 'c':
                        flags &= ~Pattern.CASE_INSENSITIVE;
                        break;
                    case 'n':
                        flags |= Pattern.DOTALL;
                        break;
                    case 'm':
                        flags |= Pattern.MULTILINE;
                        break;
                    default:
                        throw RESOURCE.invalidInputForRegexpReplace(stringFlags).ex();
                }
            }
            return flags;
        }

        /** SQL {@code RLIKE} function. */
        public boolean rlike(String s, String pattern) {
            return cache.getUnchecked(new Key(0, pattern)).matcher(s).find();
        }
    }

    /** SQL {@code LPAD(string, integer, string)} function. */
    public static String lpad(String originalValue, int returnLength, String pattern) {
        if (returnLength < 0) {
            throw RESOURCE.illegalNegativePadLength().ex();
        }
        if (pattern.isEmpty()) {
            throw RESOURCE.illegalEmptyPadPattern().ex();
        }
        if (returnLength <= originalValue.length()) {
            return originalValue.substring(0, returnLength);
        }
        int paddingLengthRequired = returnLength - originalValue.length();
        int patternLength = pattern.length();
        final StringBuilder paddedS = new StringBuilder();
        for (int i = 0; i < paddingLengthRequired; i++) {
            char curChar = pattern.charAt(i % patternLength);
            paddedS.append(curChar);
        }
        paddedS.append(originalValue);
        return paddedS.toString();
    }

    /** SQL {@code LPAD(string, integer)} function. */
    public static String lpad(String originalValue, int returnLength) {
        return lpad(originalValue, returnLength, " ");
    }

    /** SQL {@code LPAD(binary, integer, binary)} function. */
    public static ByteString lpad(ByteString originalValue, int returnLength, ByteString pattern) {
        if (returnLength < 0) {
            throw RESOURCE.illegalNegativePadLength().ex();
        }
        if (pattern.length() == 0) {
            throw RESOURCE.illegalEmptyPadPattern().ex();
        }
        if (returnLength <= originalValue.length()) {
            return originalValue.substring(0, returnLength);
        }
        int paddingLengthRequired = returnLength - originalValue.length();
        int patternLength = pattern.length();
        byte[] bytes = new byte[returnLength];
        for (int i = 0; i < paddingLengthRequired; i++) {
            byte curByte = pattern.byteAt(i % patternLength);
            bytes[i] = curByte;
        }
        for (int i = paddingLengthRequired; i < returnLength; i++) {
            bytes[i] = originalValue.byteAt(i - paddingLengthRequired);
        }

        return new ByteString(bytes);
    }

    /** SQL {@code LPAD(binary, integer, binary)} function. */
    public static ByteString lpad(ByteString originalValue, int returnLength) {
        // 0x20 is the hexadecimal character for space ' '
        return lpad(originalValue, returnLength, SINGLE_SPACE_BYTE_STRING);
    }

    /** SQL {@code RPAD(string, integer, string)} function. */
    public static String rpad(String originalValue, int returnLength, String pattern) {
        if (returnLength < 0) {
            throw RESOURCE.illegalNegativePadLength().ex();
        }
        if (pattern.isEmpty()) {
            throw RESOURCE.illegalEmptyPadPattern().ex();
        }
        if (returnLength <= originalValue.length()) {
            return originalValue.substring(0, returnLength);
        }
        int paddingLengthRequired = returnLength - originalValue.length();
        int patternLength = pattern.length();
        final StringBuilder paddedS = new StringBuilder();
        paddedS.append(originalValue);
        for (int i = 0; i < paddingLengthRequired; i++) {
            char curChar = pattern.charAt(i % patternLength);
            paddedS.append(curChar);
        }
        return paddedS.toString();
    }

    /** SQL {@code RPAD(string, integer)} function. */
    public static String rpad(String originalValue, int returnLength) {
        return rpad(originalValue, returnLength, " ");
    }

    /** SQL {@code RPAD(binary, integer, binary)} function. */
    public static ByteString rpad(ByteString originalValue, int returnLength, ByteString pattern) {
        if (returnLength < 0) {
            throw RESOURCE.illegalNegativePadLength().ex();
        }
        if (pattern.length() == 0) {
            throw RESOURCE.illegalEmptyPadPattern().ex();
        }
        int originalLength = originalValue.length();
        if (returnLength <= originalLength) {
            return originalValue.substring(0, returnLength);
        }

        int paddingLengthRequired = returnLength - originalLength;
        int patternLength = pattern.length();
        byte[] bytes = new byte[returnLength];
        for (int i = 0; i < originalLength; i++) {
            bytes[i] = originalValue.byteAt(i);
        }
        for (int i = returnLength - paddingLengthRequired; i < returnLength; i++) {
            byte curByte = pattern.byteAt(i % patternLength);
            bytes[i] = curByte;
        }
        return new ByteString(bytes);
    }

    /** SQL {@code RPAD(binary, integer)} function. */
    public static ByteString rpad(ByteString originalValue, int returnLength) {
        return rpad(originalValue, returnLength, SINGLE_SPACE_BYTE_STRING);
    }

    /** SQL {@code ENDS_WITH(string, string)} function. */
    public static boolean endsWith(String s0, String s1) {
        return s0.endsWith(s1);
    }

    /** SQL {@code ENDS_WITH(binary, binary)} function. */
    public static boolean endsWith(ByteString s0, ByteString s1) {
        return s0.endsWith(s1);
    }

    /** SQL {@code STARTS_WITH(string, string)} function. */
    public static boolean startsWith(String s0, String s1) {
        return s0.startsWith(s1);
    }

    /** SQL {@code STARTS_WITH(binary, binary)} function. */
    public static boolean startsWith(ByteString s0, ByteString s1) {
        return s0.startsWith(s1);
    }

    /** SQL {@code SPLIT(string, string)} function. */
    public static List<String> split(String s, String delimiter) {
        if (s.isEmpty()) {
            return ImmutableList.of();
        }
        if (delimiter.isEmpty()) {
            return ImmutableList.of(s); // prevent mischief
        }
        final ImmutableList.Builder<String> list = ImmutableList.builder();
        for (int i = 0; ; ) {
            int j = s.indexOf(delimiter, i);
            if (j < 0) {
                list.add(s.substring(i));
                return list.build();
            }
            list.add(s.substring(i, j));
            i = j + delimiter.length();
        }
    }

    /** SQL {@code SPLIT(string)} function. */
    public static List<String> split(String s) {
        return split(s, ",");
    }

    /** SQL {@code SPLIT(binary, binary)} function. */
    public static List<ByteString> split(ByteString s, ByteString delimiter) {
        if (s.length() == 0) {
            return ImmutableList.of();
        }
        if (delimiter.length() == 0) {
            return ImmutableList.of(s); // prevent mischief
        }
        final ImmutableList.Builder<ByteString> list = ImmutableList.builder();
        for (int i = 0; ; ) {
            int j = s.indexOf(delimiter, i);
            if (j < 0) {
                list.add(s.substring(i));
                return list.build();
            }
            list.add(s.substring(i, j));
            i = j + delimiter.length();
        }
    }

    /** SQL <code>CONTAINS_SUBSTR(rows, substr)</code> operator. */
    public static @Nullable Boolean containsSubstr(Object[] rows, String substr) {
        // If rows have null arguments, it should return TRUE if substr is found, otherwise NULL
        boolean nullFlag = false;
        for (Object row : rows) {
            if (row == null) {
                nullFlag = true;
            } else if (row instanceof Object[]) {
                return containsSubstr((Object[]) row, substr);
            } else if (row instanceof ArrayList) {
                return containsSubstr((List) row, substr);
            } else if (normalize(row.toString()).contains(normalize(substr))) {
                return true;
            }
        }
        return nullFlag ? null : false;
    }

    /** SQL <code>CONTAINS_SUBSTR(arr, substr)</code> operator. */
    public static @Nullable Boolean containsSubstr(List arr, String substr) {
        // If array has null arguments, it should return TRUE if substr is found, otherwise NULL
        boolean nullFlag = false;
        for (Object item : arr) {
            if (item == null) {
                nullFlag = true;
            }
            if (item != null && containsSubstr(item, substr)) {
                return true;
            }
        }
        return nullFlag ? null : false;
    }

    /**
     * SQL <code>CONTAINS_SUBSTR(jsonString, substr, json_scope&#61;&#62;jsonScope)</code> operator.
     */
    public static boolean containsSubstr(String jsonString, String substr, String jsonScope) {
        LinkedHashMap<String, String> map =
                (LinkedHashMap<String, String>) JsonFunctions.dejsonize(jsonString);
        assert map != null;
        Set<String> keys = map.keySet();
        Collection<String> values = map.values();
        try {
            switch (JsonScope.valueOf(jsonScope)) {
                case JSON_KEYS:
                    return keys.contains(substr);
                case JSON_KEYS_AND_VALUES:
                    return keys.contains(substr) || values.contains(substr);
                case JSON_VALUES:
                    return values.contains(substr);
                default:
                    break;
            }
        } catch (IllegalArgumentException ignored) {
            // Happens when jsonScope is not one of the legal enum values
        }
        throw new IllegalArgumentException(
                "json_scope argument must be one of: \"JSON_KEYS\", "
                        + "\"JSON_VALUES\", \"JSON_KEYS_AND_VALUES\".");
    }

    /** SQL <code>CONTAINS_SUBSTR(expr, substr)</code> operator. */
    public static boolean containsSubstr(Object expr, String substr) {
        expr = normalize(expr.toString());
        substr = normalize(substr);
        if (JsonFunctions.isJsonObject(expr.toString())) {
            return containsSubstr(expr.toString(), substr, "JSON_VALUES");
        }
        return ((String) expr).contains(substr);
    }

    /** SQL <code>CONTAINS_SUBSTR(boolean, substr)</code> operator. */
    public static boolean containsSubstr(boolean s, String substr) {
        return containsSubstr(String.valueOf(s), substr);
    }

    /** SQL <code>CONTAINS_SUBSTR(int, substr)</code> operator. */
    public static boolean containsSubstr(int s, String substr) {
        return containsSubstr(String.valueOf(s), substr);
    }

    /** SQL <code>CONTAINS_SUBSTR(long, substr)</code> operator. */
    public static boolean containsSubstr(long s, String substr) {
        return containsSubstr(String.valueOf(s), substr);
    }

    /** Helper for CONTAINS_SUBSTR. */
    private static String normalize(String s) {
        // Before values are compared, should be case folded and normalized using NFKC normalization
        s = StringEscapeUtils.unescapeJava(s);
        s = Normalizer.normalize(s, Normalizer.Form.NFKC);
        s = lower(s);
        return s;
    }

    /** SQL SUBSTRING(string FROM ...) function. */
    public static String substring(String c, int s) {
        final int s0 = s - 1;
        if (s0 <= 0) {
            return c;
        }
        if (s > c.length()) {
            return "";
        }
        return c.substring(s0);
    }

    /** SQL SUBSTRING(string FROM ... FOR ...) function. */
    public static String substring(String c, int s, int l) {
        int lc = c.length();
        long e = (long) s + (long) l;
        if (l < 0) {
            throw RESOURCE.illegalNegativeSubstringLength().ex();
        }
        // Prevent overflow in addition
        if (s > lc || e < 1L) {
            return "";
        }
        final int s0 = Math.max(s - 1, 0);
        final long e0 = Math.min(e - 1, (long) lc);
        // We know that e0 cannot exceed Integer.MAX_VALUE, since it's smaller than lc
        return c.substring(s0, (int) e0);
    }

    /** SQL SUBSTRING(binary FROM ...) function for binary. */
    public static ByteString substring(ByteString c, int s) {
        final int s0 = s - 1;
        if (s0 <= 0) {
            return c;
        }
        if (s > c.length()) {
            return ByteString.EMPTY;
        }
        return c.substring(s0);
    }

    /** SQL SUBSTRING(binary FROM ... FOR ...) function for binary. */
    public static ByteString substring(ByteString c, int s, int l) {
        int lc = c.length();
        int e = s + l;
        if (l < 0) {
            throw RESOURCE.illegalNegativeSubstringLength().ex();
        }
        if (s > lc || e < 1) {
            return ByteString.EMPTY;
        }
        final int s0 = Math.max(s - 1, 0);
        final int e0 = Math.min(e - 1, lc);
        return c.substring(s0, e0);
    }

    /** SQL FORMAT_NUMBER(value, decimalOrFormat) function. */
    public static String formatNumber(long value, int decimalVal) {
        DecimalFormat numberFormat = getNumberFormat(decimalVal);
        return numberFormat.format(value);
    }

    public static String formatNumber(double value, int decimalVal) {
        DecimalFormat numberFormat = getNumberFormat(decimalVal);
        return numberFormat.format(value);
    }

    public static String formatNumber(BigDecimal value, int decimalVal) {
        DecimalFormat numberFormat = getNumberFormat(decimalVal);
        return numberFormat.format(value);
    }

    public static String formatNumber(long value, String format) {
        DecimalFormat numberFormat = getNumberFormat(format);
        return numberFormat.format(value);
    }

    public static String formatNumber(double value, String format) {
        DecimalFormat numberFormat = getNumberFormat(format);
        return numberFormat.format(value);
    }

    public static String formatNumber(BigDecimal value, String format) {
        DecimalFormat numberFormat = getNumberFormat(format);
        return numberFormat.format(value);
    }

    public static String getFormatPattern(int decimalVal) {
        StringBuilder pattern = new StringBuilder();
        pattern.append("#,###,###,###,###,###,##0");

        if (decimalVal > 0) {
            pattern.append(".");
            for (int i = 0; i < decimalVal; i++) {
                pattern.append("0");
            }
        }
        return pattern.toString();
    }

    private static DecimalFormat getNumberFormat(String pattern) {
        return NumberUtil.decimalFormat(pattern);
    }

    private static DecimalFormat getNumberFormat(int decimalVal) {
        if (decimalVal < 0) {
            throw RESOURCE.illegalNegativeDecimalValue().ex();
        }
        String pattern = getFormatPattern(decimalVal);
        return getNumberFormat(pattern);
    }

    /** SQL UPPER(string) function. */
    public static String upper(String s) {
        return s.toUpperCase(Locale.ROOT);
    }

    /** SQL LOWER(string) function. */
    public static String lower(String s) {
        return s.toLowerCase(Locale.ROOT);
    }

    /** SQL INITCAP(string) function. */
    public static String initcap(String s) {
        // Assumes Alpha as [A-Za-z0-9]
        // white space is treated as everything else.
        final int len = s.length();
        boolean start = true;
        final StringBuilder newS = new StringBuilder();

        for (int i = 0; i < len; i++) {
            char curCh = s.charAt(i);
            final int c = (int) curCh;
            if (start) { // curCh is whitespace or first character of word.
                if (c > 47 && c < 58) { // 0-9
                    start = false;
                } else if (c > 64 && c < 91) { // A-Z
                    start = false;
                } else if (c > 96 && c < 123) { // a-z
                    start = false;
                    curCh = (char) (c - 32); // Uppercase this character
                }
                // else {} whitespace
            } else { // Inside of a word or white space after end of word.
                if (c > 47 && c < 58) { // 0-9
                    // noop
                } else if (c > 64 && c < 91) { // A-Z
                    curCh = (char) (c + 32); // Lowercase this character
                } else if (c > 96 && c < 123) { // a-z
                    // noop
                } else { // whitespace
                    start = true;
                }
            }
            newS.append(curCh);
        } // for each character in s
        return newS.toString();
    }

    /** SQL REVERSE(string) function. */
    public static String reverse(String s) {
        final StringBuilder buf = new StringBuilder(s);
        return buf.reverse().toString();
    }

    /** SQL LEVENSHTEIN(string1, string2) function. */
    public static int levenshtein(String string1, String string2) {
        return LEVENSHTEIN_DISTANCE.apply(string1, string2);
    }

    /**
     * SQL FIND_IN_SET(matchStr, textStr) function. Returns the index (1-based) of the given
     * matchStr in the comma-delimited list textStr. Returns 0, if the matchStr is not found or if
     * the matchStr contains a comma.
     */
    public static @Nullable Integer findInSet(@Nullable String matchStr, @Nullable String textStr) {
        if (matchStr == null || textStr == null) {
            return null;
        }
        if (matchStr.contains(COMMA_DELIMITER)) {
            return 0;
        }
        String[] splits = textStr.split(COMMA_DELIMITER);
        for (int i = 0; i < splits.length; i++) {
            if (matchStr.equals(splits[i])) {
                return i + 1;
            }
        }
        return 0;
    }

    /** SQL ASCII(string) function. */
    public static int ascii(String s) {
        return s.isEmpty() ? 0 : s.codePointAt(0);
    }

    /** SQL REPEAT(string, int) function. */
    public static String repeat(String s, int n) {
        if (n < 1) {
            return "";
        }
        return Strings.repeat(s, n);
    }

    /** SQL SPACE(int) function. */
    public static String space(int n) {
        return repeat(" ", n);
    }

    /** SQL STRCMP(String,String) function. */
    public static int strcmp(String s0, String s1) {
        return (int) Math.signum(s1.compareTo(s0));
    }

    /** SQL SOUNDEX(string) function. */
    public static String soundex(String s) {
        return SOUNDEX.soundex(s);
    }

    /** SQL SOUNDEX(string) function but return original s when not mapped. */
    public static String soundexSpark(String s) {
        try {
            return SOUNDEX.soundex(s);
        } catch (IllegalArgumentException ignore) {
            return s;
        }
    }

    /** SQL DIFFERENCE(string, string) function. */
    public static int difference(String s0, String s1) {
        String result0 = soundex(s0);
        String result1 = soundex(s1);
        for (int i = 0; i < SOUNDEX_LENGTH; i++) {
            if (result0.charAt(i) != result1.charAt(i)) {
                return i;
            }
        }
        return SOUNDEX_LENGTH;
    }

    /** SQL LEFT(string, integer) function. */
    public static String left(String s, int n) {
        if (n <= 0) {
            return "";
        }
        int len = s.length();
        if (n >= len) {
            return s;
        }
        return s.substring(0, n);
    }

    /** SQL LEFT(ByteString, integer) function. */
    public static ByteString left(ByteString s, int n) {
        if (n <= 0) {
            return ByteString.EMPTY;
        }
        int len = s.length();
        if (n >= len) {
            return s;
        }
        return s.substring(0, n);
    }

    /** SQL RIGHT(string, integer) function. */
    public static String right(String s, int n) {
        if (n <= 0) {
            return "";
        }
        int len = s.length();
        if (n >= len) {
            return s;
        }
        return s.substring(len - n);
    }

    /** SQL RIGHT(ByteString, integer) function. */
    public static ByteString right(ByteString s, int n) {
        if (n <= 0) {
            return ByteString.EMPTY;
        }
        final int len = s.length();
        if (n >= len) {
            return s;
        }
        return s.substring(len - n);
    }

    /**
     * SQL CHAR(integer) function, as in MySQL and Spark.
     *
     * <p>Returns the ASCII character of {@code n} modulo 256, or null if {@code n} &lt; 0.
     */
    public static @Nullable String charFromAscii(int n) {
        if (n < 0) {
            return null;
        }
        return String.valueOf(Character.toChars(n % 256));
    }

    /**
     * SQL CHR(integer) function, as in Oracle and Postgres.
     *
     * <p>Returns the UTF-8 character whose code is {@code n}.
     */
    public static String charFromUtf8(int n) {
        return String.valueOf(Character.toChars(n));
    }

    /** SQL CODE_POINTS_TO_BYTES(list) function. */
    public static @Nullable ByteString codePointsToBytes(List codePoints) {
        int length = codePoints.size();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            Object codePoint = codePoints.get(i);
            if (codePoint == null) {
                return null;
            }
            assert codePoint instanceof Number;
            long cp = ((Number) codePoint).longValue();
            if (cp < 0 || cp > 255) {
                throw RESOURCE.inputArgumentsOfFunctionOutOfRange(
                                "CODE_POINTS_TO_BYTES", cp, "[0, 255]")
                        .ex();
            }
            bytes[i] = (byte) cp;
        }

        return new ByteString(bytes);
    }

    /** SQL CODE_POINTS_TO_STRING(list) function. */
    public static @Nullable String codePointsToString(List codePoints) {
        StringBuilder sb = new StringBuilder();
        for (Object codePoint : codePoints) {
            if (codePoint == null) {
                return null;
            }
            assert codePoint instanceof Number;
            long cp = ((Number) codePoint).longValue();
            // Each valid code point should fall within the range of [0, 0xD7FF] and [0xE000,
            // 0x10FFFF]
            if (cp >= 0 && cp <= 0xD7FF || cp >= 0xE000 && cp <= 0x10FFFF) {
                sb.append(charFromUtf8((int) cp));
            } else {
                throw RESOURCE.inputArgumentsOfFunctionOutOfRange(
                                "CODE_POINTS_TO_STRING", cp, "[0, 0xD7FF] and [0xE000, 0x10FFFF]")
                        .ex();
            }
        }

        return sb.toString();
    }

    /** SQL TO_CODE_POINTS(string) function. */
    public static @Nullable List<Integer> toCodePoints(String s) {
        if (s.length() == 0) {
            return null;
        }
        final ImmutableList.Builder<Integer> builder = new ImmutableList.Builder<>();
        final int length = s.length();
        int i = 0;
        while (i < length) {
            int cp = s.codePointAt(i);
            builder.add(cp);
            i += cp == s.charAt(i) ? 1 : 2;
        }
        return builder.build();
    }

    /** SQL TO_CODE_POINTS(string) function for binary string. */
    public static @Nullable List<Integer> toCodePoints(ByteString s) {
        if (s.length() == 0) {
            return null;
        }
        final ImmutableList.Builder<Integer> builder = new ImmutableList.Builder<>();
        for (byte b : s.getBytes()) {
            builder.add((int) b);
        }
        return builder.build();
    }

    /** SQL OCTET_LENGTH(binary) function. */
    public static int octetLength(ByteString s) {
        return s.length();
    }

    /** SQL CHARACTER_LENGTH(string) function. */
    public static int charLength(String s) {
        return s.length();
    }

    /** SQL BIT_LENGTH(string) function. */
    public static int bitLength(String s) {
        return s.getBytes(UTF_8).length * 8;
    }

    /** SQL BIT_LENGTH(binary) function. */
    public static int bitLength(ByteString s) {
        return s.length() * 8;
    }

    /** SQL BIT_GET(value, position) function. */
    public static byte bitGet(long value, int position) {
        checkPosition(position, java.lang.Long.SIZE - 1);
        return (byte) ((value >> position) & 1);
    }

    /** SQL BIT_GET(value, position) function. */
    public static byte bitGet(int value, int position) {
        checkPosition(position, java.lang.Integer.SIZE - 1);
        return (byte) ((value >> position) & 1);
    }

    /** SQL BIT_GET(value, position) function. */
    public static byte bitGet(short value, int position) {
        checkPosition(position, java.lang.Short.SIZE - 1);
        return (byte) ((value >> position) & 1);
    }

    /** SQL BIT_GET(value, position) function. */
    public static byte bitGet(byte value, int position) {
        checkPosition(position, java.lang.Byte.SIZE - 1);
        return (byte) ((value >> position) & 1);
    }

    private static void checkPosition(int position, int limit) {
        if (position < 0) {
            throw RESOURCE.illegalNegativeBitGetPosition(position).ex();
        }
        if (limit < position) {
            throw RESOURCE.illegalBitGetPositionExceedsLimit(position, limit).ex();
        }
    }

    /** SQL {@code string || string} operator. */
    public static String concat(String s0, String s1) {
        return s0 + s1;
    }

    /**
     * Concatenates two strings. Returns null only when both s0 and s1 are null, otherwise null is
     * treated as empty string.
     */
    public static @Nullable String concatWithNull(@Nullable String s0, @Nullable String s1) {
        if (s0 == null) {
            return s1;
        } else if (s1 == null) {
            return s0;
        } else {
            return s0 + s1;
        }
    }

    /** SQL {@code binary || binary} operator. */
    public static ByteString concat(ByteString s0, ByteString s1) {
        return s0.concat(s1);
    }

    /** SQL {@code CONCAT(arg0, arg1, arg2, ...)} function. */
    public static String concatMulti(String... args) {
        return String.join("", args);
    }

    /**
     * SQL {@code CONCAT(arg0, ...)} function which can accept null but never return null. Always
     * treats null as empty string.
     */
    public static String concatMultiWithNull(String... args) {
        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            sb.append(arg == null ? "" : arg);
        }
        return sb.toString();
    }

    /**
     * SQL {@code CONCAT_WS(sep, arg1, arg2, ...)} function; treats null arguments as empty strings.
     */
    public static String concatMultiWithSeparator(String... args) {
        // the separator arg could be null
        final String sep = args[0] == null ? "" : args[0];
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < args.length; i++) {
            if (args[i] != null) {
                if (i < args.length - 1) {
                    sb.append(args[i]).append(sep);
                } else {
                    // no separator after the last arg
                    sb.append(args[i]);
                }
            }
        }
        return sb.toString();
    }

    /** SQL {@code CONVERT(s, src_charset, dest_charset)} function. */
    public static String convertWithCharset(String s, String srcCharset, String destCharset) {
        final Charset src = SqlUtil.getCharset(srcCharset);
        final Charset dest = SqlUtil.getCharset(destCharset);
        byte[] bytes = s.getBytes(src);
        final CharsetDecoder decoder = dest.newDecoder();
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            return decoder.decode(buffer).toString();
        } catch (CharacterCodingException ex) {
            throw RESOURCE.charsetEncoding(s, dest.name()).ex();
        }
    }

    /**
     * SQL {@code TRANSLATE(s USING transcodingName)} function, also known as {@code CONVERT(s USING
     * transcodingName)}.
     */
    public static String translateWithCharset(String s, String transcodingName) {
        final Charset charset = SqlUtil.getCharset(transcodingName);
        byte[] bytes = s.getBytes(Charset.defaultCharset());
        final CharsetDecoder decoder = charset.newDecoder();
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            return decoder.decode(buffer).toString();
        } catch (CharacterCodingException ex) {
            throw RESOURCE.charsetEncoding(s, charset.name()).ex();
        }
    }

    /** State for {@code PARSE_URL}. */
    @Deterministic
    public static class ParseUrlFunction {
        static Pattern keyToPattern(String keyToExtract) {
            return Pattern.compile("(&|^)" + keyToExtract + "=([^&]*)");
        }

        private final LoadingCache<String, Pattern> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(CacheLoader.from(ParseUrlFunction::keyToPattern));

        /** SQL {@code PARSE_URL(urlStr, partToExtract, keyToExtract)} function. */
        public @Nullable String parseUrl(String urlStr, String partToExtract, String keyToExtract) {
            if (!partToExtract.equals("QUERY")) {
                return null;
            }

            String query = parseUrl(urlStr, partToExtract);
            if (query == null) {
                return null;
            }

            Pattern p = cache.getUnchecked(keyToExtract);
            Matcher m = p.matcher(query);
            return m.find() ? m.group(2) : null;
        }

        /** SQL {@code PARSE_URL(urlStr, partToExtract)} function. */
        public @Nullable String parseUrl(String urlStr, String partToExtract) {
            URI uri;
            try {
                uri = new URI(urlStr);
            } catch (URISyntaxException e) {
                return null;
            }

            PartToExtract part;
            try {
                part = PartToExtract.valueOf(partToExtract);
            } catch (IllegalArgumentException e) {
                return null;
            }

            switch (part) {
                case HOST:
                    return uri.getHost();
                case PATH:
                    return uri.getRawPath();
                case QUERY:
                    return uri.getRawQuery();
                case REF:
                    return uri.getRawFragment();
                case PROTOCOL:
                    return uri.getScheme();
                case FILE:
                    if (uri.getRawQuery() != null) {
                        return uri.getRawPath() + "?" + uri.getRawQuery();
                    } else {
                        return uri.getRawPath();
                    }
                case AUTHORITY:
                    return uri.getRawAuthority();
                case USERINFO:
                    return uri.getRawUserInfo();
                default:
                    return null;
            }
        }
    }

    /** SQL {@code RTRIM} function applied to string. */
    public static String rtrim(String s) {
        return trim(false, true, " ", s);
    }

    /** SQL {@code LTRIM} function. */
    public static String ltrim(String s) {
        return trim(true, false, " ", s);
    }

    /** SQL {@code TRIM(... seek FROM s)} function. */
    public static String trim(boolean left, boolean right, String seek, String s) {
        return trim(left, right, seek, s, true);
    }

    public static String trim(boolean left, boolean right, String seek, String s, boolean strict) {
        if (strict && seek.length() != 1) {
            throw RESOURCE.trimError().ex();
        }
        int j = s.length();
        if (right) {
            for (; ; ) {
                if (j == 0) {
                    return "";
                }
                if (seek.indexOf(s.charAt(j - 1)) < 0) {
                    break;
                }
                --j;
            }
        }
        int i = 0;
        if (left) {
            for (; ; ) {
                if (i == j) {
                    return "";
                }
                if (seek.indexOf(s.charAt(i)) < 0) {
                    break;
                }
                ++i;
            }
        }
        return s.substring(i, j);
    }

    /** SQL {@code TRIM} function applied to binary string. */
    public static ByteString trim(ByteString s) {
        return trim_(s, true, true);
    }

    /** Helper for CAST. */
    public static ByteString rtrim(ByteString s) {
        return trim_(s, false, true);
    }

    /** SQL {@code TRIM} function applied to binary string. */
    private static ByteString trim_(ByteString s, boolean left, boolean right) {
        int j = s.length();
        if (right) {
            for (; ; ) {
                if (j == 0) {
                    return ByteString.EMPTY;
                }
                if (s.byteAt(j - 1) != 0) {
                    break;
                }
                --j;
            }
        }
        int i = 0;
        if (left) {
            for (; ; ) {
                if (i == j) {
                    return ByteString.EMPTY;
                }
                if (s.byteAt(i) != 0) {
                    break;
                }
                ++i;
            }
        }
        return s.substring(i, j);
    }

    /** SQL {@code OVERLAY} function. */
    public static String overlay(String s, String r, int start) {
        return s.substring(0, start - 1) + r + s.substring(start - 1 + r.length());
    }

    /** SQL {@code OVERLAY} function. */
    public static String overlay(String s, String r, int start, int length) {
        return s.substring(0, start - 1) + r + s.substring(start - 1 + length);
    }

    /** SQL {@code OVERLAY} function applied to binary strings. */
    public static ByteString overlay(ByteString s, ByteString r, int start) {
        return s.substring(0, start - 1).concat(r).concat(s.substring(start - 1 + r.length()));
    }

    /** SQL {@code OVERLAY} function applied to binary strings. */
    public static ByteString overlay(ByteString s, ByteString r, int start, int length) {
        return s.substring(0, start - 1).concat(r).concat(s.substring(start - 1 + length));
    }

    /** State for {@code LIKE}, {@code ILIKE}. */
    @Deterministic
    public static class LikeFunction {
        /** Key for cache of compiled regular expressions. */
        private static final class Key {
            final String pattern;
            final @Nullable String escape;
            final int flags;

            Key(String pattern, @Nullable String escape, int flags) {
                this.pattern = pattern;
                this.escape = escape;
                this.flags = flags;
            }

            @Override
            public int hashCode() {
                return pattern.hashCode() ^ (escape == null ? 0 : escape.hashCode()) ^ flags;
            }

            @Override
            public boolean equals(@Nullable Object obj) {
                return this == obj
                        || obj instanceof Key
                                && pattern.equals(((Key) obj).pattern)
                                && Objects.equals(escape, ((Key) obj).escape)
                                && flags == ((Key) obj).flags;
            }

            Pattern toPattern() {
                String regex = Like.sqlToRegexLike(pattern, escape);
                return Pattern.compile(regex, flags);
            }
        }

        private final LoadingCache<Key, Pattern> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(CacheLoader.from(Key::toPattern));

        /** SQL {@code LIKE} function. */
        public boolean like(String s, String pattern) {
            final Key key = new Key(pattern, null, 0);
            return cache.getUnchecked(key).matcher(s).matches();
        }

        /** SQL {@code LIKE} function with escape. */
        public boolean like(String s, String pattern, String escape) {
            final Key key = new Key(pattern, escape, 0);
            return cache.getUnchecked(key).matcher(s).matches();
        }

        /** SQL {@code ILIKE} function. */
        public boolean ilike(String s, String pattern) {
            final Key key = new Key(pattern, null, Pattern.CASE_INSENSITIVE);
            return cache.getUnchecked(key).matcher(s).matches();
        }

        /** SQL {@code ILIKE} function with escape. */
        public boolean ilike(String s, String pattern, String escape) {
            final Key key = new Key(pattern, escape, Pattern.CASE_INSENSITIVE);
            return cache.getUnchecked(key).matcher(s).matches();
        }
    }

    /** State for {@code SIMILAR} function. */
    @Deterministic
    public static class SimilarFunction {
        private final LoadingCache<String, Pattern> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(
                                CacheLoader.from(
                                        pattern ->
                                                Pattern.compile(
                                                        Like.sqlToRegexSimilar(pattern, null))));

        /** SQL {@code SIMILAR} function. */
        public boolean similar(String s, String pattern) {
            return cache.getUnchecked(pattern).matcher(s).matches();
        }
    }

    /** State for {@code SIMILAR} function with escape. */
    public static class SimilarEscapeFunction {
        /** Cache key. */
        private static class Key extends MapEntry<String, String> {
            Key(String formatModel, String format) {
                super(formatModel, format);
            }

            Pattern toPattern() {
                return Pattern.compile(Like.sqlToRegexSimilar(t, u));
            }
        }

        private final LoadingCache<Key, Pattern> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(CacheLoader.from(Key::toPattern));

        /** SQL {@code SIMILAR} function with escape. */
        public boolean similar(String s, String pattern, String escape) {
            return cache.getUnchecked(new Key(pattern, escape)).matcher(s).matches();
        }
    }

    /** State for posix regex function. */
    @Deterministic
    public static class PosixRegexFunction {
        private final LoadingCache<Ord<String>, Pattern> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(
                                CacheLoader.from(
                                        pattern -> Like.posixRegexToPattern(pattern.e, pattern.i)));

        boolean posixRegex(String s, String regex, int flags) {
            final Ord<String> key = Ord.of(flags, regex);
            return cache.getUnchecked(key).matcher(s).find();
        }

        /** Posix regex, case-insensitive. */
        public boolean posixRegexInsensitive(String s, String regex) {
            return posixRegex(s, regex, Pattern.CASE_INSENSITIVE);
        }

        /** Posix regex, case-sensitive. */
        public boolean posixRegexSensitive(String s, String regex) {
            return posixRegex(s, regex, 0);
        }
    }

    // =

    /** SQL <code>=</code> operator applied to BigDecimal values (neither may be null). */
    public static boolean eq(BigDecimal b0, BigDecimal b1) {
        return b0.stripTrailingZeros().equals(b1.stripTrailingZeros());
    }

    /** SQL <code>=</code> operator applied to Object[] values (neither may be null). */
    public static boolean eq(@Nullable Object[] b0, @Nullable Object[] b1) {
        return Arrays.deepEquals(b0, b1);
    }

    /**
     * SQL <code>=</code> operator applied to Object values (including String; neither side may be
     * null).
     */
    public static boolean eq(Object b0, Object b1) {
        return b0.equals(b1);
    }

    /** SQL <code>=</code> operator applied to String values with a certain Comparator. */
    public static boolean eq(String s0, String s1, Comparator<String> comparator) {
        return comparator.compare(s0, s1) == 0;
    }

    /**
     * SQL <code>=</code> operator applied to Object values (at least one operand has ANY type;
     * neither may be null).
     */
    public static boolean eqAny(Object b0, Object b1) {
        if (b0.getClass().equals(b1.getClass())) {
            // The result of SqlFunctions.eq(BigDecimal, BigDecimal) makes more sense
            // than BigDecimal.equals(BigDecimal). So if both of types are BigDecimal,
            // we just use SqlFunctions.eq(BigDecimal, BigDecimal).
            if (BigDecimal.class.isInstance(b0)) {
                return eq((BigDecimal) b0, (BigDecimal) b1);
            } else {
                return b0.equals(b1);
            }
        } else if (allAssignable(Number.class, b0, b1)) {
            return eq(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }
        // We shouldn't rely on implementation even though overridden equals can
        // handle other types which may create worse result: for example,
        // a.equals(b) != b.equals(a)
        return false;
    }

    /** Returns whether two objects can both be assigned to a given class. */
    private static boolean allAssignable(Class clazz, Object o0, Object o1) {
        return clazz.isInstance(o0) && clazz.isInstance(o1);
    }

    // <>

    /** SQL <code>&lt;gt;</code> operator applied to BigDecimal values. */
    public static boolean ne(BigDecimal b0, BigDecimal b1) {
        return b0.compareTo(b1) != 0;
    }

    /**
     * SQL <code>&lt;gt;</code> operator applied to Object values (including String; neither side
     * may be null).
     */
    public static boolean ne(Object b0, Object b1) {
        return !eq(b0, b1);
    }

    /** SQL <code>&lt;gt;</code> operator applied to OString values with a certain Comparator. */
    public static boolean ne(String s0, String s1, Comparator<String> comparator) {
        return !eq(s0, s1, comparator);
    }

    /**
     * SQL <code>&lt;gt;</code> operator applied to Object values (at least one operand has ANY
     * type, including String; neither may be null).
     */
    public static boolean neAny(Object b0, Object b1) {
        return !eqAny(b0, b1);
    }

    // <

    /** SQL <code>&lt;</code> operator applied to boolean values. */
    public static boolean lt(boolean b0, boolean b1) {
        return Boolean.compare(b0, b1) < 0;
    }

    /** SQL <code>&lt;</code> operator applied to String values. */
    public static boolean lt(String b0, String b1) {
        return b0.compareTo(b1) < 0;
    }

    /** SQL <code>&lt;</code> operator applied to String values. */
    public static boolean lt(String b0, String b1, Comparator<String> comparator) {
        return comparator.compare(b0, b1) < 0;
    }

    /** SQL <code>&lt;</code> operator applied to ByteString values. */
    public static boolean lt(ByteString b0, ByteString b1) {
        return b0.compareTo(b1) < 0;
    }

    /** SQL <code>&lt;</code> operator applied to BigDecimal values. */
    public static boolean lt(BigDecimal b0, BigDecimal b1) {
        return b0.compareTo(b1) < 0;
    }

    /**
     * Returns whether {@code b0} is less than {@code b1} (or {@code b1} is null). Helper for {@code
     * ARG_MIN}.
     */
    public static <T extends Comparable<T>> boolean ltNullable(T b0, T b1) {
        return b1 == null || b0 != null && b0.compareTo(b1) < 0;
    }

    public static boolean lt(byte b0, byte b1) {
        return b0 < b1;
    }

    public static boolean lt(char b0, char b1) {
        return b0 < b1;
    }

    public static boolean lt(short b0, short b1) {
        return b0 < b1;
    }

    public static boolean lt(int b0, int b1) {
        return b0 < b1;
    }

    public static boolean lt(long b0, long b1) {
        return b0 < b1;
    }

    public static boolean lt(float b0, float b1) {
        return b0 < b1;
    }

    public static boolean lt(double b0, double b1) {
        return b0 < b1;
    }

    /** SQL <code>&lt;</code> operator applied to Object values. */
    public static boolean ltAny(Object b0, Object b1) {
        if (b0.getClass().equals(b1.getClass()) && b0 instanceof Comparable) {
            //noinspection unchecked
            return ((Comparable) b0).compareTo(b1) < 0;
        } else if (allAssignable(Number.class, b0, b1)) {
            return lt(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notComparable("<", b0, b1);
    }

    // <=

    /** SQL <code>&le;</code> operator applied to boolean values. */
    public static boolean le(boolean b0, boolean b1) {
        return Boolean.compare(b0, b1) <= 0;
    }

    /** SQL <code>&le;</code> operator applied to String values. */
    public static boolean le(String b0, String b1) {
        return b0.compareTo(b1) <= 0;
    }

    /** SQL <code>&le;</code> operator applied to String values. */
    public static boolean le(String b0, String b1, Comparator<String> comparator) {
        return comparator.compare(b0, b1) <= 0;
    }

    /** SQL <code>&le;</code> operator applied to ByteString values. */
    public static boolean le(ByteString b0, ByteString b1) {
        return b0.compareTo(b1) <= 0;
    }

    /** SQL <code>&le;</code> operator applied to BigDecimal values. */
    public static boolean le(BigDecimal b0, BigDecimal b1) {
        return b0.compareTo(b1) <= 0;
    }

    /**
     * SQL <code>&le;</code> operator applied to Object values (at least one operand has ANY type;
     * neither may be null).
     */
    public static boolean leAny(Object b0, Object b1) {
        if (b0.getClass().equals(b1.getClass()) && b0 instanceof Comparable) {
            //noinspection unchecked
            return ((Comparable) b0).compareTo(b1) <= 0;
        } else if (allAssignable(Number.class, b0, b1)) {
            return le(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notComparable("<=", b0, b1);
    }

    // >

    /** SQL <code>&gt;</code> operator applied to boolean values. */
    public static boolean gt(boolean b0, boolean b1) {
        return Boolean.compare(b0, b1) > 0;
    }

    /** SQL <code>&gt;</code> operator applied to String values. */
    public static boolean gt(String b0, String b1) {
        return b0.compareTo(b1) > 0;
    }

    /** SQL <code>&gt;</code> operator applied to String values. */
    public static boolean gt(String b0, String b1, Comparator<String> comparator) {
        return comparator.compare(b0, b1) > 0;
    }

    /** SQL <code>&gt;</code> operator applied to ByteString values. */
    public static boolean gt(ByteString b0, ByteString b1) {
        return b0.compareTo(b1) > 0;
    }

    /** SQL <code>&gt;</code> operator applied to BigDecimal values. */
    public static boolean gt(BigDecimal b0, BigDecimal b1) {
        return b0.compareTo(b1) > 0;
    }

    /**
     * Returns whether {@code b0} is greater than {@code b1} (or {@code b1} is null). Helper for
     * {@code ARG_MAX}.
     */
    public static <T extends Comparable<T>> boolean gtNullable(T b0, T b1) {
        return b1 == null || b0 != null && b0.compareTo(b1) > 0;
    }

    public static boolean gt(byte b0, byte b1) {
        return b0 > b1;
    }

    public static boolean gt(char b0, char b1) {
        return b0 > b1;
    }

    public static boolean gt(short b0, short b1) {
        return b0 > b1;
    }

    public static boolean gt(int b0, int b1) {
        return b0 > b1;
    }

    public static boolean gt(long b0, long b1) {
        return b0 > b1;
    }

    public static boolean gt(float b0, float b1) {
        return b0 > b1;
    }

    public static boolean gt(double b0, double b1) {
        return b0 > b1;
    }

    /**
     * SQL <code>&gt;</code> operator applied to Object values (at least one operand has ANY type;
     * neither may be null).
     */
    public static boolean gtAny(Object b0, Object b1) {
        if (b0.getClass().equals(b1.getClass()) && b0 instanceof Comparable) {
            //noinspection unchecked
            return ((Comparable) b0).compareTo(b1) > 0;
        } else if (allAssignable(Number.class, b0, b1)) {
            return gt(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notComparable(">", b0, b1);
    }

    // >=

    /** SQL <code>&ge;</code> operator applied to boolean values. */
    public static boolean ge(boolean b0, boolean b1) {
        return Boolean.compare(b0, b1) >= 0;
    }

    /** SQL <code>&ge;</code> operator applied to String values. */
    public static boolean ge(String b0, String b1) {
        return b0.compareTo(b1) >= 0;
    }

    /** SQL <code>&ge;</code> operator applied to String values. */
    public static boolean ge(String b0, String b1, Comparator<String> comparator) {
        return comparator.compare(b0, b1) >= 0;
    }

    /** SQL <code>&ge;</code> operator applied to ByteString values. */
    public static boolean ge(ByteString b0, ByteString b1) {
        return b0.compareTo(b1) >= 0;
    }

    /** SQL <code>&ge;</code> operator applied to BigDecimal values. */
    public static boolean ge(BigDecimal b0, BigDecimal b1) {
        return b0.compareTo(b1) >= 0;
    }

    /**
     * SQL <code>&ge;</code> operator applied to Object values (at least one operand has ANY type;
     * neither may be null).
     */
    public static boolean geAny(Object b0, Object b1) {
        if (b0.getClass().equals(b1.getClass()) && b0 instanceof Comparable) {
            //noinspection unchecked
            return ((Comparable) b0).compareTo(b1) >= 0;
        } else if (allAssignable(Number.class, b0, b1)) {
            return ge(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notComparable(">=", b0, b1);
    }

    // +

    /** SQL <code>+</code> operator applied to int values. */
    public static int plus(int b0, int b1) {
        return b0 + b1;
    }

    /** SQL <code>+</code> operator applied to int values; left side may be null. */
    public static Integer plus(Integer b0, int b1) {
        return b0 == null ? castNonNull(null) : (b0 + b1);
    }

    /** SQL <code>+</code> operator applied to int values; right side may be null. */
    public static Integer plus(int b0, Integer b1) {
        return b1 == null ? castNonNull(null) : (b0 + b1);
    }

    /** SQL <code>+</code> operator applied to nullable int values. */
    public static Integer plus(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0 + b1);
    }

    /** SQL <code>+</code> operator applied to nullable long and int values. */
    public static Long plus(Long b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() + b1.longValue());
    }

    /** SQL <code>+</code> operator applied to nullable int and long values. */
    public static Long plus(Integer b0, Long b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() + b1.longValue());
    }

    /** SQL <code>+</code> operator applied to BigDecimal values. */
    public static BigDecimal plus(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : b0.add(b1);
    }

    /**
     * SQL <code>+</code> operator applied to Object values (at least one operand has ANY type;
     * either may be null).
     */
    public static Object plusAny(Object b0, Object b1) {
        if (b0 == null || b1 == null) {
            return castNonNull(null);
        }

        if (allAssignable(Number.class, b0, b1)) {
            return plus(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notArithmetic("+", b0, b1);
    }

    // -

    /** SQL <code>-</code> operator applied to int values. */
    public static int minus(int b0, int b1) {
        return b0 - b1;
    }

    /** SQL <code>-</code> operator applied to int values; left side may be null. */
    public static Integer minus(Integer b0, int b1) {
        return b0 == null ? castNonNull(null) : (b0 - b1);
    }

    /** SQL <code>-</code> operator applied to int values; right side may be null. */
    public static Integer minus(int b0, Integer b1) {
        return b1 == null ? castNonNull(null) : (b0 - b1);
    }

    /** SQL <code>-</code> operator applied to nullable int values. */
    public static Integer minus(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0 - b1);
    }

    /** SQL <code>-</code> operator applied to nullable long and int values. */
    public static Long minus(Long b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() - b1.longValue());
    }

    /** SQL <code>-</code> operator applied to nullable int and long values. */
    public static Long minus(Integer b0, Long b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() - b1.longValue());
    }

    /** SQL <code>-</code> operator applied to nullable BigDecimal values. */
    public static BigDecimal minus(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : b0.subtract(b1);
    }

    /**
     * SQL <code>-</code> operator applied to Object values (at least one operand has ANY type;
     * either may be null).
     */
    public static Object minusAny(Object b0, Object b1) {
        if (b0 == null || b1 == null) {
            return castNonNull(null);
        }

        if (allAssignable(Number.class, b0, b1)) {
            return minus(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notArithmetic("-", b0, b1);
    }

    // /

    /** SQL <code>/</code> operator applied to int values. */
    public static int divide(int b0, int b1) {
        return b0 / b1;
    }

    /** SQL <code>/</code> operator applied to int values; left side may be null. */
    public static Integer divide(Integer b0, int b1) {
        return b0 == null ? castNonNull(null) : (b0 / b1);
    }

    /** SQL <code>/</code> operator applied to int values; right side may be null. */
    public static Integer divide(int b0, Integer b1) {
        return b1 == null ? castNonNull(null) : (b0 / b1);
    }

    /** SQL <code>/</code> operator applied to nullable int values. */
    public static Integer divide(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0 / b1);
    }

    /** SQL <code>/</code> operator applied to nullable long and int values. */
    public static Long divide(Long b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() / b1.longValue());
    }

    /** SQL <code>/</code> operator applied to nullable int and long values. */
    public static Long divide(Integer b0, Long b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() / b1.longValue());
    }

    /** SQL <code>/</code> operator applied to BigDecimal values. */
    public static BigDecimal divide(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null)
                ? castNonNull(null)
                : b0.divide(b1, MathContext.DECIMAL64);
    }

    /**
     * SQL <code>/</code> operator applied to Object values (at least one operand has ANY type;
     * either may be null).
     */
    public static Object divideAny(Object b0, Object b1) {
        if (b0 == null || b1 == null) {
            return castNonNull(null);
        }

        if (allAssignable(Number.class, b0, b1)) {
            return divide(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notArithmetic("/", b0, b1);
    }

    public static int divide(int b0, BigDecimal b1) {
        return BigDecimal.valueOf(b0).divide(b1, RoundingMode.HALF_DOWN).intValue();
    }

    public static long divide(long b0, BigDecimal b1) {
        return BigDecimal.valueOf(b0).divide(b1, RoundingMode.HALF_DOWN).longValue();
    }

    // *

    /** SQL <code>*</code> operator applied to int values. */
    public static int multiply(int b0, int b1) {
        return b0 * b1;
    }

    /** SQL <code>*</code> operator applied to int values; left side may be null. */
    public static Integer multiply(Integer b0, int b1) {
        return b0 == null ? castNonNull(null) : (b0 * b1);
    }

    /** SQL <code>*</code> operator applied to int values; right side may be null. */
    public static Integer multiply(int b0, Integer b1) {
        return b1 == null ? castNonNull(null) : (b0 * b1);
    }

    /** SQL <code>*</code> operator applied to nullable int values. */
    public static Integer multiply(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0 * b1);
    }

    /** SQL <code>*</code> operator applied to nullable long and int values. */
    public static Long multiply(Long b0, Integer b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() * b1.longValue());
    }

    /** SQL <code>*</code> operator applied to nullable int and long values. */
    public static Long multiply(Integer b0, Long b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : (b0.longValue() * b1.longValue());
    }

    /** SQL <code>*</code> operator applied to nullable BigDecimal values. */
    public static BigDecimal multiply(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null) ? castNonNull(null) : b0.multiply(b1);
    }

    /**
     * SQL <code>*</code> operator applied to Object values (at least one operand has ANY type;
     * either may be null).
     */
    public static Object multiplyAny(Object b0, Object b1) {
        if (b0 == null || b1 == null) {
            return castNonNull(null);
        }

        if (allAssignable(Number.class, b0, b1)) {
            return multiply(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }

        throw notArithmetic("*", b0, b1);
    }

    /** SQL <code>SAFE_ADD</code> function applied to long values. */
    public static @Nullable Long safeAdd(long b0, long b1) {
        try {
            return Math.addExact(b0, b1);
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /** SQL <code>SAFE_ADD</code> function applied to long and BigDecimal values. */
    public static @Nullable BigDecimal safeAdd(long b0, BigDecimal b1) {
        BigDecimal ans = BigDecimal.valueOf(b0).add(b1);
        return safeDecimal(ans) ? ans : null;
    }

    /** SQL <code>SAFE_ADD</code> function applied to BigDecimal and long values. */
    public static @Nullable BigDecimal safeAdd(BigDecimal b0, long b1) {
        return safeAdd(b1, b0);
    }

    /** SQL <code>SAFE_ADD</code> function applied to BigDecimal values. */
    public static @Nullable BigDecimal safeAdd(BigDecimal b0, BigDecimal b1) {
        BigDecimal ans = b0.add(b1);
        return safeDecimal(ans) ? ans : null;
    }

    /** SQL <code>SAFE_ADD</code> function applied to double and long values. */
    public static @Nullable Double safeAdd(double b0, long b1) {
        double ans = b0 + b1;
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_ADD</code> function applied to long and double values. */
    public static @Nullable Double safeAdd(long b0, double b1) {
        return safeAdd(b1, b0);
    }

    /** SQL <code>SAFE_ADD</code> function applied to double and BigDecimal values. */
    public static @Nullable Double safeAdd(double b0, BigDecimal b1) {
        double ans = b0 + b1.doubleValue();
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_ADD</code> function applied to BigDecimal and double values. */
    public static @Nullable Double safeAdd(BigDecimal b0, double b1) {
        return safeAdd(b1, b0);
    }

    /** SQL <code>SAFE_ADD</code> function applied to double values. */
    public static @Nullable Double safeAdd(double b0, double b1) {
        double ans = b0 + b1;
        boolean isFinite = Double.isFinite(b0) && Double.isFinite(b1);
        return safeDouble(ans) || !isFinite ? ans : null;
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to long values. */
    public static @Nullable Double safeDivide(long b0, long b1) {
        double ans = (double) b0 / b1;
        return safeDouble(ans) && b1 != 0 ? ans : null;
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to long and BigDecimal values. */
    public static @Nullable BigDecimal safeDivide(long b0, BigDecimal b1) {
        try {
            BigDecimal ans = BigDecimal.valueOf(b0).divide(b1);
            return safeDecimal(ans) ? ans : null;
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to BigDecimal and long values. */
    public static @Nullable BigDecimal safeDivide(BigDecimal b0, long b1) {
        try {
            BigDecimal ans = b0.divide(BigDecimal.valueOf(b1));
            return safeDecimal(ans) ? ans : null;
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to BigDecimal values. */
    public static @Nullable BigDecimal safeDivide(BigDecimal b0, BigDecimal b1) {
        try {
            BigDecimal ans = b0.divide(b1);
            return safeDecimal(ans) ? ans : null;
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to double and long values. */
    public static @Nullable Double safeDivide(double b0, long b1) {
        double ans = b0 / b1;
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to long and double values. */
    public static @Nullable Double safeDivide(long b0, double b1) {
        double ans = b0 / b1;
        return safeDouble(ans) || !Double.isFinite(b1) ? ans : null;
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to double and BigDecimal values. */
    public static @Nullable Double safeDivide(double b0, BigDecimal b1) {
        double ans = b0 / b1.doubleValue();
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to BigDecimal and double values. */
    public static @Nullable Double safeDivide(BigDecimal b0, double b1) {
        double ans = b0.doubleValue() / b1;
        return safeDouble(ans) || !Double.isFinite(b1) ? ans : null;
    }

    /** SQL <code>SAFE_DIVIDE</code> function applied to double values. */
    public static @Nullable Double safeDivide(double b0, double b1) {
        double ans = b0 / b1;
        boolean isFinite = Double.isFinite(b0) && Double.isFinite(b1);
        return safeDouble(ans) || !isFinite ? ans : null;
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to long values. */
    public static @Nullable Long safeMultiply(long b0, long b1) {
        try {
            return Math.multiplyExact(b0, b1);
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to long and BigDecimal values. */
    public static @Nullable BigDecimal safeMultiply(long b0, BigDecimal b1) {
        BigDecimal ans = BigDecimal.valueOf(b0).multiply(b1);
        return safeDecimal(ans) ? ans : null;
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to BigDecimal and long values. */
    public static @Nullable BigDecimal safeMultiply(BigDecimal b0, long b1) {
        return safeMultiply(b1, b0);
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to BigDecimal values. */
    public static @Nullable BigDecimal safeMultiply(BigDecimal b0, BigDecimal b1) {
        BigDecimal ans = b0.multiply(b1);
        return safeDecimal(ans) ? ans : null;
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to double and long values. */
    public static @Nullable Double safeMultiply(double b0, long b1) {
        double ans = b0 * b1;
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to long and double values. */
    public static @Nullable Double safeMultiply(long b0, double b1) {
        return safeMultiply(b1, b0);
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to double and BigDecimal values. */
    public static @Nullable Double safeMultiply(double b0, BigDecimal b1) {
        double ans = b0 * b1.doubleValue();
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to BigDecimal and double values. */
    public static @Nullable Double safeMultiply(BigDecimal b0, double b1) {
        return safeMultiply(b1, b0);
    }

    /** SQL <code>SAFE_MULTIPLY</code> function applied to double values. */
    public static @Nullable Double safeMultiply(double b0, double b1) {
        double ans = b0 * b1;
        boolean isFinite = Double.isFinite(b0) && Double.isFinite(b1);
        return safeDouble(ans) || !isFinite ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to long values. */
    public static @Nullable Long safeSubtract(long b0, long b1) {
        try {
            return Math.subtractExact(b0, b1);
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to long and BigDecimal values. */
    public static @Nullable BigDecimal safeSubtract(long b0, BigDecimal b1) {
        BigDecimal ans = BigDecimal.valueOf(b0).subtract(b1);
        return safeDecimal(ans) ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to BigDecimal and long values. */
    public static @Nullable BigDecimal safeSubtract(BigDecimal b0, long b1) {
        BigDecimal ans = b0.subtract(BigDecimal.valueOf(b1));
        return safeDecimal(ans) ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to BigDecimal values. */
    public static @Nullable BigDecimal safeSubtract(BigDecimal b0, BigDecimal b1) {
        BigDecimal ans = b0.subtract(b1);
        return safeDecimal(ans) ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to double and long values. */
    public static @Nullable Double safeSubtract(double b0, long b1) {
        double ans = b0 - b1;
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to long and double values. */
    public static @Nullable Double safeSubtract(long b0, double b1) {
        double ans = b0 - b1;
        return safeDouble(ans) || !Double.isFinite(b1) ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to double and BigDecimal values. */
    public static @Nullable Double safeSubtract(double b0, BigDecimal b1) {
        double ans = b0 - b1.doubleValue();
        return safeDouble(ans) || !Double.isFinite(b0) ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to BigDecimal and double values. */
    public static @Nullable Double safeSubtract(BigDecimal b0, double b1) {
        double ans = b0.doubleValue() - b1;
        return safeDouble(ans) || !Double.isFinite(b1) ? ans : null;
    }

    /** SQL <code>SAFE_SUBTRACT</code> function applied to double values. */
    public static @Nullable Double safeSubtract(double b0, double b1) {
        double ans = b0 - b1;
        boolean isFinite = Double.isFinite(b0) && Double.isFinite(b1);
        return safeDouble(ans) || !isFinite ? ans : null;
    }

    /**
     * Returns whether a BigDecimal value is safe (that is, has not overflowed). According to
     * BigQuery, BigDecimal overflow occurs if the precision is greater than 76 or the scale is
     * greater than 38.
     */
    private static boolean safeDecimal(BigDecimal b) {
        return b.scale() <= 38 && b.precision() <= 76;
    }

    /** Returns whether a double value is safe (that is, has not overflowed). */
    private static boolean safeDouble(double d) {
        // If the double is positive and falls between the MIN and MAX double values,
        // overflow has not occurred. If the double is negative and falls between the
        // negated MIN and MAX double values, overflow has not occurred. Otherwise,
        // overflow has occurred. Important to note that 'Double.MIN_VALUE' refers to
        // minimum positive value.
        return Math.abs(d) > Double.MIN_VALUE && Math.abs(d) < Double.MAX_VALUE || d == 0;
    }

    private static RuntimeException notArithmetic(String op, Object b0, Object b1) {
        return RESOURCE.invalidTypesForArithmetic(
                        b0.getClass().toString(), op, b1.getClass().toString())
                .ex();
    }

    private static RuntimeException notComparable(String op, Object b0, Object b1) {
        return RESOURCE.invalidTypesForComparison(
                        b0.getClass().toString(), op, b1.getClass().toString())
                .ex();
    }

    /** Bitwise function <code>BIT_AND</code> applied to integer values. */
    public static long bitAnd(long b0, long b1) {
        return b0 & b1;
    }

    /** Bitwise function <code>BIT_AND</code> applied to binary values. */
    public static ByteString bitAnd(ByteString b0, ByteString b1) {
        return binaryOperator(b0, b1, (x, y) -> (byte) (x & y));
    }

    /** Bitwise function <code>BIT_OR</code> applied to integer values. */
    public static long bitOr(long b0, long b1) {
        return b0 | b1;
    }

    /** Bitwise function <code>BIT_OR</code> applied to binary values. */
    public static ByteString bitOr(ByteString b0, ByteString b1) {
        return binaryOperator(b0, b1, (x, y) -> (byte) (x | y));
    }

    /** Bitwise function <code>BIT_XOR</code> applied to integer values. */
    public static long bitXor(long b0, long b1) {
        return b0 ^ b1;
    }

    /** Bitwise function <code>BIT_XOR</code> applied to binary values. */
    public static ByteString bitXor(ByteString b0, ByteString b1) {
        return binaryOperator(b0, b1, (x, y) -> (byte) (x ^ y));
    }

    /**
     * Utility for bitwise function applied to two byteString values.
     *
     * @param b0 The first byteString value operand of bitwise function.
     * @param b1 The second byteString value operand of bitwise function.
     * @param bitOp BitWise binary operator.
     * @return ByteString after bitwise operation.
     */
    private static ByteString binaryOperator(
            ByteString b0, ByteString b1, BinaryOperator<Byte> bitOp) {
        if (b0.length() == 0) {
            return b1;
        }
        if (b1.length() == 0) {
            return b0;
        }

        if (b0.length() != b1.length()) {
            throw RESOURCE.differentLengthForBitwiseOperands(b0.length(), b1.length()).ex();
        }

        final byte[] result = new byte[b0.length()];
        for (int i = 0; i < b0.length(); i++) {
            result[i] = bitOp.apply(b0.byteAt(i), b1.byteAt(i));
        }

        return new ByteString(result);
    }

    // EXP

    /** SQL <code>EXP</code> operator applied to double values. */
    public static double exp(double b0) {
        return Math.exp(b0);
    }

    public static double exp(BigDecimal b0) {
        return Math.exp(b0.doubleValue());
    }

    // POWER

    /** SQL <code>POWER</code> operator applied to double values. */
    public static double power(double b0, double b1) {
        return Math.pow(b0, b1);
    }

    public static double power(double b0, BigDecimal b1) {
        return Math.pow(b0, b1.doubleValue());
    }

    public static double power(BigDecimal b0, double b1) {
        return Math.pow(b0.doubleValue(), b1);
    }

    public static double power(BigDecimal b0, BigDecimal b1) {
        return Math.pow(b0.doubleValue(), b1.doubleValue());
    }

    // LN, LOG, LOG10

    /** SQL {@code LOG(number, number2)} function applied to double values. */
    public static double log(double d0, double d1) {
        return Math.log(d0) / Math.log(d1);
    }

    /** SQL {@code LOG(number, number2)} function applied to double and BigDecimal values. */
    public static double log(double d0, BigDecimal d1) {
        return Math.log(d0) / Math.log(d1.doubleValue());
    }

    /** SQL {@code LOG(number, number2)} function applied to BigDecimal and double values. */
    public static double log(BigDecimal d0, double d1) {
        return Math.log(d0.doubleValue()) / Math.log(d1);
    }

    /** SQL {@code LOG(number, number2)} function applied to double values. */
    public static double log(BigDecimal d0, BigDecimal d1) {
        return Math.log(d0.doubleValue()) / Math.log(d1.doubleValue());
    }

    // MOD

    /** SQL <code>MOD</code> operator applied to byte values. */
    public static byte mod(byte b0, byte b1) {
        return (byte) (b0 % b1);
    }

    /** SQL <code>MOD</code> operator applied to short values. */
    public static short mod(short b0, short b1) {
        return (short) (b0 % b1);
    }

    /** SQL <code>MOD</code> operator applied to int values. */
    public static int mod(int b0, int b1) {
        return b0 % b1;
    }

    /** SQL <code>MOD</code> operator applied to long values. */
    public static long mod(long b0, long b1) {
        return b0 % b1;
    }

    // temporary
    public static BigDecimal mod(BigDecimal b0, int b1) {
        return mod(b0, BigDecimal.valueOf(b1));
    }

    // temporary
    public static BigDecimal mod(int b0, BigDecimal b1) {
        return mod(BigDecimal.valueOf(b0), b1);
    }

    public static BigDecimal mod(BigDecimal b0, BigDecimal b1) {
        final BigDecimal[] bigDecimals = b0.divideAndRemainder(b1);
        return bigDecimals[1];
    }

    // FLOOR

    public static double floor(double b0) {
        return Math.floor(b0);
    }

    public static float floor(float b0) {
        return (float) Math.floor(b0);
    }

    public static BigDecimal floor(BigDecimal b0) {
        return b0.setScale(0, RoundingMode.FLOOR);
    }

    /** SQL <code>FLOOR</code> operator applied to byte values. */
    public static byte floor(byte b0, byte b1) {
        return (byte) floor((int) b0, (int) b1);
    }

    /** SQL <code>FLOOR</code> operator applied to short values. */
    public static short floor(short b0, short b1) {
        return (short) floor((int) b0, (int) b1);
    }

    /** SQL <code>FLOOR</code> operator applied to int values. */
    public static int floor(int b0, int b1) {
        int r = b0 % b1;
        if (r < 0) {
            r += b1;
        }
        return b0 - r;
    }

    /** SQL <code>FLOOR</code> operator applied to long values. */
    public static long floor(long b0, long b1) {
        long r = b0 % b1;
        if (r < 0) {
            r += b1;
        }
        return b0 - r;
    }

    // temporary
    public static BigDecimal floor(BigDecimal b0, int b1) {
        return floor(b0, BigDecimal.valueOf(b1));
    }

    // temporary
    public static int floor(int b0, BigDecimal b1) {
        return floor(b0, b1.intValue());
    }

    public static BigDecimal floor(BigDecimal b0, BigDecimal b1) {
        final BigDecimal[] bigDecimals = b0.divideAndRemainder(b1);
        BigDecimal r = bigDecimals[1];
        if (r.signum() < 0) {
            r = r.add(b1);
        }
        return b0.subtract(r);
    }

    // CEIL

    public static double ceil(double b0) {
        return Math.ceil(b0);
    }

    public static float ceil(float b0) {
        return (float) Math.ceil(b0);
    }

    public static BigDecimal ceil(BigDecimal b0) {
        return b0.setScale(0, RoundingMode.CEILING);
    }

    /** SQL <code>CEIL</code> operator applied to byte values. */
    public static byte ceil(byte b0, byte b1) {
        return floor((byte) (b0 + b1 - 1), b1);
    }

    /** SQL <code>CEIL</code> operator applied to short values. */
    public static short ceil(short b0, short b1) {
        return floor((short) (b0 + b1 - 1), b1);
    }

    /** SQL <code>CEIL</code> operator applied to int values. */
    public static int ceil(int b0, int b1) {
        int r = b0 % b1;
        if (r > 0) {
            r -= b1;
        }
        return b0 - r;
    }

    /** SQL <code>CEIL</code> operator applied to long values. */
    public static long ceil(long b0, long b1) {
        return floor(b0 + b1 - 1, b1);
    }

    // temporary
    public static BigDecimal ceil(BigDecimal b0, int b1) {
        return ceil(b0, BigDecimal.valueOf(b1));
    }

    // temporary
    public static int ceil(int b0, BigDecimal b1) {
        return ceil(b0, b1.intValue());
    }

    public static BigDecimal ceil(BigDecimal b0, BigDecimal b1) {
        final BigDecimal[] bigDecimals = b0.divideAndRemainder(b1);
        BigDecimal r = bigDecimals[1];
        if (r.signum() > 0) {
            r = r.subtract(b1);
        }
        return b0.subtract(r);
    }

    // ABS

    /** SQL <code>ABS</code> operator applied to byte values. */
    public static byte abs(byte b0) {
        return (byte) Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to short values. */
    public static short abs(short b0) {
        return (short) Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to int values. */
    public static int abs(int b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to long values. */
    public static long abs(long b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to float values. */
    public static float abs(float b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to double values. */
    public static double abs(double b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to BigDecimal values. */
    public static BigDecimal abs(BigDecimal b0) {
        return b0.abs();
    }

    // ACOS
    /** SQL <code>ACOS</code> operator applied to BigDecimal values. */
    public static double acos(BigDecimal b0) {
        return Math.acos(b0.doubleValue());
    }

    /** SQL <code>ACOS</code> operator applied to double values. */
    public static double acos(double b0) {
        return Math.acos(b0);
    }

    // ACOSH
    /** SQL <code>ACOSH</code> operator applied to BigDecimal values. */
    public static double acosh(BigDecimal b0) {
        return acosh(b0.doubleValue());
    }

    /** SQL <code>ACOSH</code> operator applied to double values. */
    public static double acosh(double b0) {
        if (b0 < 1) {
            throw new IllegalArgumentException("Input parameter of acosh cannot be less than 1!");
        }
        return Math.log(Math.sqrt(b0 * b0 - 1.0d) + b0);
    }

    // ASIN
    /** SQL <code>ASIN</code> operator applied to BigDecimal values. */
    public static double asin(BigDecimal b0) {
        return Math.asin(b0.doubleValue());
    }

    /** SQL <code>ASIN</code> operator applied to double values. */
    public static double asin(double b0) {
        return Math.asin(b0);
    }

    // ASINH
    /** SQL <code>ASINH</code> operator applied to BigDecimal values. */
    public static double asinh(BigDecimal b0) {
        return asinh(b0.doubleValue());
    }

    /** SQL <code>ASINH</code> operator applied to double values. */
    public static double asinh(double b0) {
        final double sign;
        // check the sign bit of the raw representation to handle -0.
        if (Double.doubleToRawLongBits(b0) < 0) {
            b0 = Math.abs(b0);
            sign = -1.0d;
        } else {
            sign = 1.0d;
        }
        return sign * Math.log(Math.sqrt(b0 * b0 + 1.0d) + b0);
    }

    // ATAN
    /** SQL <code>ATAN</code> operator applied to BigDecimal values. */
    public static double atan(BigDecimal b0) {
        return Math.atan(b0.doubleValue());
    }

    /** SQL <code>ATAN</code> operator applied to double values. */
    public static double atan(double b0) {
        return Math.atan(b0);
    }

    // ATAN2
    /** SQL <code>ATAN2</code> operator applied to double/BigDecimal values. */
    public static double atan2(double b0, BigDecimal b1) {
        return Math.atan2(b0, b1.doubleValue());
    }

    /** SQL <code>ATAN2</code> operator applied to BigDecimal/double values. */
    public static double atan2(BigDecimal b0, double b1) {
        return Math.atan2(b0.doubleValue(), b1);
    }

    /** SQL <code>ATAN2</code> operator applied to BigDecimal values. */
    public static double atan2(BigDecimal b0, BigDecimal b1) {
        return Math.atan2(b0.doubleValue(), b1.doubleValue());
    }

    /** SQL <code>ATAN2</code> operator applied to double values. */
    public static double atan2(double b0, double b1) {
        return Math.atan2(b0, b1);
    }

    // ATANH
    /** SQL <code>ATANH</code> operator applied to BigDecimal values. */
    public static double atanh(BigDecimal b) {
        return atanh(b.doubleValue());
    }

    /** SQL <code>ATANH</code> operator applied to double values. */
    public static double atanh(double b) {
        if (Math.abs(b) >= 1) {
            throw RESOURCE.inputArgumentsOfFunctionOutOfRange("ATANH", b, "(-1, 1)").ex();
        }
        final double mult;
        // check the sign bit of the raw representation to handle -0.
        if (Double.doubleToRawLongBits(b) < 0) {
            b = Math.abs(b);
            mult = -0.5d;
        } else {
            mult = 0.5d;
        }
        return mult * Math.log((1.0d + b) / (1.0d - b));
    }

    // CBRT
    /** SQL <code>CBRT</code> operator applied to BigDecimal values. */
    public static double cbrt(BigDecimal b) {
        return cbrt(b.doubleValue());
    }

    /** SQL <code>CBRT</code> operator applied to double values. */
    public static double cbrt(double b) {
        return Math.cbrt(b);
    }

    // COS
    /** SQL <code>COS</code> operator applied to BigDecimal values. */
    public static double cos(BigDecimal b0) {
        return Math.cos(b0.doubleValue());
    }

    /** SQL <code>COS</code> operator applied to double values. */
    public static double cos(double b0) {
        return Math.cos(b0);
    }

    // COSH
    /** SQL <code>COSH</code> operator applied to BigDecimal values. */
    public static double cosh(BigDecimal b) {
        return cosh(b.doubleValue());
    }

    /** SQL <code>COSH</code> operator applied to double values. */
    public static double cosh(double b) {
        return Math.cosh(b);
    }

    // COT
    /** SQL <code>COT</code> operator applied to BigDecimal values. */
    public static double cot(BigDecimal b0) {
        return 1.0d / Math.tan(b0.doubleValue());
    }

    /** SQL <code>COT</code> operator applied to double values. */
    public static double cot(double b0) {
        return 1.0d / Math.tan(b0);
    }

    /** SQL <code>COTH</code> operator applied to BigDecimal values. */
    public static double coth(BigDecimal b0) {
        return 1.0d / Math.tanh(b0.doubleValue());
    }

    /** SQL <code>COTH</code> operator applied to double values. */
    public static double coth(double b0) {
        return 1.0d / Math.tanh(b0);
    }

    /** SQL <code>CSCH</code> operator applied to BigDecimal values. */
    public static double csch(BigDecimal b0) {
        return 1.0d / Math.sinh(b0.doubleValue());
    }

    /** SQL <code>CSCH</code> operator applied to double values. */
    public static double csch(double b0) {
        return 1.0d / Math.sinh(b0);
    }

    // DEGREES
    /** SQL <code>DEGREES</code> operator applied to BigDecimal values. */
    public static double degrees(BigDecimal b0) {
        return Math.toDegrees(b0.doubleValue());
    }

    /** SQL <code>DEGREES</code> operator applied to double values. */
    public static double degrees(double b0) {
        return Math.toDegrees(b0);
    }

    /** SQL <code>FACTORIAL</code> operator. */
    public static @Nullable Long factorial(int b0) {
        if (b0 < 0 || b0 > 20) {
            return null;
        }
        return CombinatoricsUtils.factorial(b0);
    }

    /** SQL <code>IS_INF</code> operator applied to BigDecimal values. */
    public static boolean isInf(BigDecimal b0) {
        return Double.isInfinite(b0.doubleValue());
    }

    /** SQL <code>IS_INF</code> operator applied to double values. */
    public static boolean isInf(double b0) {
        return Double.isInfinite(b0);
    }

    /** SQL <code>IS_INF</code> operator applied to float values. */
    public static boolean isInf(float b0) {
        return Float.isInfinite(b0);
    }

    /** SQL <code>IS_NAN</code> operator applied to BigDecimal values. */
    public static boolean isNaN(BigDecimal b0) {
        return Double.isNaN(b0.doubleValue());
    }

    /** SQL <code>IS_NAN</code> operator applied to double values. */
    public static boolean isNaN(double b0) {
        return Double.isNaN(b0);
    }

    /** SQL <code>IS_NAN</code> operator applied to float values. */
    public static boolean isNaN(float b0) {
        return Float.isNaN(b0);
    }

    // RADIANS
    /** SQL <code>RADIANS</code> operator applied to BigDecimal values. */
    public static double radians(BigDecimal b0) {
        return Math.toRadians(b0.doubleValue());
    }

    /** SQL <code>RADIANS</code> operator applied to double values. */
    public static double radians(double b0) {
        return Math.toRadians(b0);
    }

    /** SQL <code>SECH</code> operator applied to BigDecimal values. */
    public static double sech(BigDecimal b0) {
        return 1.0d / Math.cosh(b0.doubleValue());
    }

    /** SQL <code>SECH</code> operator applied to double values. */
    public static double sech(double b0) {
        return 1.0d / Math.cosh(b0);
    }

    // SQL ROUND
    /** SQL <code>ROUND</code> operator applied to int values. */
    public static int sround(int b0) {
        return sround(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to int values. */
    public static int sround(int b0, int b1) {
        return sround(BigDecimal.valueOf(b0), b1).intValue();
    }

    /** SQL <code>ROUND</code> operator applied to long values. */
    public static long sround(long b0) {
        return sround(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to long values. */
    public static long sround(long b0, int b1) {
        return sround(BigDecimal.valueOf(b0), b1).longValue();
    }

    /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
    public static BigDecimal sround(BigDecimal b0) {
        return sround(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
    public static BigDecimal sround(BigDecimal b0, int b1) {
        return b0.movePointRight(b1).setScale(0, RoundingMode.HALF_UP).movePointLeft(b1);
    }

    /** SQL <code>ROUND</code> operator applied to double values. */
    public static double sround(double b0) {
        return sround(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to double values. */
    public static double sround(double b0, int b1) {
        return sround(BigDecimal.valueOf(b0), b1).doubleValue();
    }

    // SQL TRUNCATE
    /** SQL <code>TRUNCATE</code> operator applied to int values. */
    public static int struncate(int b0) {
        return struncate(b0, 0);
    }

    public static int struncate(int b0, int b1) {
        return struncate(BigDecimal.valueOf(b0), b1).intValue();
    }

    /** SQL <code>TRUNCATE</code> operator applied to long values. */
    public static long struncate(long b0) {
        return struncate(b0, 0);
    }

    public static long struncate(long b0, int b1) {
        return struncate(BigDecimal.valueOf(b0), b1).longValue();
    }

    /** SQL <code>TRUNCATE</code> operator applied to BigDecimal values. */
    public static BigDecimal struncate(BigDecimal b0) {
        return struncate(b0, 0);
    }

    public static BigDecimal struncate(BigDecimal b0, int b1) {
        return b0.movePointRight(b1).setScale(0, RoundingMode.DOWN).movePointLeft(b1);
    }

    /** SQL <code>TRUNCATE</code> operator applied to double values. */
    public static double struncate(double b0) {
        return struncate(b0, 0);
    }

    public static double struncate(double b0, int b1) {
        return struncate(BigDecimal.valueOf(b0), b1).doubleValue();
    }

    // SIGN
    /** SQL <code>SIGN</code> operator applied to int values. */
    public static int sign(int b0) {
        return Integer.signum(b0);
    }

    /** SQL <code>SIGN</code> operator applied to long values. */
    public static long sign(long b0) {
        return Long.signum(b0);
    }

    /** SQL <code>SIGN</code> operator applied to BigDecimal values. */
    public static BigDecimal sign(BigDecimal b0) {
        return BigDecimal.valueOf(b0.signum());
    }

    /** SQL <code>SIGN</code> operator applied to double values. */
    public static double sign(double b0) {
        return Math.signum(b0);
    }

    // SIN
    /** SQL <code>SIN</code> operator applied to BigDecimal values. */
    public static double sin(BigDecimal b0) {
        return Math.sin(b0.doubleValue());
    }

    /** SQL <code>SIN</code> operator applied to double values. */
    public static double sin(double b0) {
        return Math.sin(b0);
    }

    // SINH
    /** SQL <code>SINH</code> operator applied to BigDecimal values. */
    public static double sinh(BigDecimal b) {
        return sinh(b.doubleValue());
    }

    /** SQL <code>SINH</code> operator applied to double values. */
    public static double sinh(double b) {
        return Math.sinh(b);
    }

    // TAN
    /** SQL <code>TAN</code> operator applied to BigDecimal values. */
    public static double tan(BigDecimal b0) {
        return Math.tan(b0.doubleValue());
    }

    /** SQL <code>TAN</code> operator applied to double values. */
    public static double tan(double b0) {
        return Math.tan(b0);
    }

    // TANH
    /** SQL <code>TANH</code> operator applied to BigDecimal values. */
    public static double tanh(BigDecimal b) {
        return tanh(b.doubleValue());
    }

    /** SQL <code>TANH</code> operator applied to double values. */
    public static double tanh(double b) {
        return Math.tanh(b);
    }

    // CSC
    /** SQL <code>CSC</code> operator applied to BigDecimal values. */
    public static double csc(BigDecimal b0) {
        return 1.0d / Math.sin(b0.doubleValue());
    }

    /** SQL <code>CSC</code> operator applied to double values. */
    public static double csc(double b0) {
        return 1.0d / Math.sin(b0);
    }

    // SEC
    /** SQL <code>SEC</code> operator applied to BigDecimal values. */
    public static double sec(BigDecimal b0) {
        return 1.0d / Math.cos(b0.doubleValue());
    }

    /** SQL <code>SEC</code> operator applied to double values. */
    public static double sec(double b0) {
        return 1.0d / Math.cos(b0);
    }

    // Helpers

    /** Helper for implementing MIN. Somewhat similar to LEAST operator. */
    public static <T extends Comparable<T>> T lesser(T b0, T b1) {
        return b0 == null || b0.compareTo(b1) > 0 ? b1 : b0;
    }

    /** LEAST operator. */
    public static <T extends Comparable<T>> T least(T b0, T b1) {
        return b0 == null || b1 != null && b0.compareTo(b1) > 0 ? b1 : b0;
    }

    public static boolean greater(boolean b0, boolean b1) {
        return b0 || b1;
    }

    public static boolean lesser(boolean b0, boolean b1) {
        return b0 && b1;
    }

    public static byte greater(byte b0, byte b1) {
        return b0 > b1 ? b0 : b1;
    }

    public static byte lesser(byte b0, byte b1) {
        return b0 > b1 ? b1 : b0;
    }

    public static char greater(char b0, char b1) {
        return b0 > b1 ? b0 : b1;
    }

    public static char lesser(char b0, char b1) {
        return b0 > b1 ? b1 : b0;
    }

    public static short greater(short b0, short b1) {
        return b0 > b1 ? b0 : b1;
    }

    public static short lesser(short b0, short b1) {
        return b0 > b1 ? b1 : b0;
    }

    public static int greater(int b0, int b1) {
        return b0 > b1 ? b0 : b1;
    }

    public static int lesser(int b0, int b1) {
        return b0 > b1 ? b1 : b0;
    }

    public static long greater(long b0, long b1) {
        return b0 > b1 ? b0 : b1;
    }

    public static long lesser(long b0, long b1) {
        return b0 > b1 ? b1 : b0;
    }

    public static float greater(float b0, float b1) {
        return b0 > b1 ? b0 : b1;
    }

    public static float lesser(float b0, float b1) {
        return b0 > b1 ? b1 : b0;
    }

    public static double greater(double b0, double b1) {
        return b0 > b1 ? b0 : b1;
    }

    public static double lesser(double b0, double b1) {
        return b0 > b1 ? b1 : b0;
    }

    /** Helper for implementing MAX. Somewhat similar to GREATEST operator. */
    public static <T extends Comparable<T>> T greater(T b0, T b1) {
        return b0 == null || b0.compareTo(b1) < 0 ? b1 : b0;
    }

    /** GREATEST operator. */
    public static <T extends Comparable<T>> T greatest(T b0, T b1) {
        return b0 == null || b1 != null && b0.compareTo(b1) < 0 ? b1 : b0;
    }

    /** CAST(FLOAT AS VARCHAR). */
    public static String toString(float x) {
        if (x == 0) {
            return "0E0";
        }
        BigDecimal bigDecimal = new BigDecimal(x, MathContext.DECIMAL32).stripTrailingZeros();
        final String s = bigDecimal.toString();
        return PATTERN_0_STAR_E.matcher(s).replaceAll("E").replace("E+", "E");
    }

    /** CAST(DOUBLE AS VARCHAR). */
    public static String toString(double x) {
        if (x == 0) {
            return "0E0";
        }
        BigDecimal bigDecimal = new BigDecimal(x, MathContext.DECIMAL64).stripTrailingZeros();
        final String s = bigDecimal.toString();
        return PATTERN_0_STAR_E.matcher(s).replaceAll("E").replace("E+", "E");
    }

    /** CAST(DECIMAL AS VARCHAR). */
    public static String toString(BigDecimal x) {
        final String s = x.toString();
        if (s.equals("0")) {
            return s;
        } else if (s.startsWith("0.")) {
            // we want ".1" not "0.1"
            return s.substring(1);
        } else if (s.startsWith("-0.")) {
            // we want "-.1" not "-0.1"
            return "-" + s.substring(2);
        } else {
            return s;
        }
    }

    /** CAST(BOOLEAN AS VARCHAR). */
    public static String toString(boolean x) {
        // Boolean.toString returns lower case -- no good.
        return x ? "TRUE" : "FALSE";
    }

    @NonDeterministic
    private static Object cannotConvert(Object o, Class toType) {
        throw RESOURCE.cannotConvert(String.valueOf(o), toType.toString()).ex();
    }

    /** CAST(VARCHAR AS BOOLEAN). */
    public static boolean toBoolean(String s) {
        s = trim(true, true, " ", s);
        if (s.equalsIgnoreCase("TRUE")) {
            return true;
        } else if (s.equalsIgnoreCase("FALSE")) {
            return false;
        } else {
            throw RESOURCE.invalidCharacterForCast(s).ex();
        }
    }

    public static boolean toBoolean(Number number) {
        return !number.equals(0);
    }

    public static boolean toBoolean(Object o) {
        return o instanceof Boolean
                ? (Boolean) o
                : o instanceof Number
                        ? toBoolean((Number) o)
                        : o instanceof String
                                ? toBoolean((String) o)
                                : (Boolean) cannotConvert(o, boolean.class);
    }

    // Don't need parseByte etc. - Byte.parseByte is sufficient.

    public static byte toByte(Object o) {
        return o instanceof Byte
                ? (Byte) o
                : o instanceof Number ? toByte((Number) o) : Byte.parseByte(o.toString());
    }

    public static byte toByte(Number number) {
        return number.byteValue();
    }

    public static char toChar(String s) {
        return s.charAt(0);
    }

    public static Character toCharBoxed(String s) {
        return s.charAt(0);
    }

    public static short toShort(String s) {
        return Short.parseShort(s.trim());
    }

    public static short toShort(Number number) {
        return number.shortValue();
    }

    public static short toShort(Object o) {
        return o instanceof Short
                ? (Short) o
                : o instanceof Number
                        ? toShort((Number) o)
                        : o instanceof String
                                ? toShort((String) o)
                                : (Short) cannotConvert(o, short.class);
    }

    /**
     * Converts a SQL DATE value from the Java type ({@link java.sql.Date}) to the internal
     * representation type (number of days since January 1st, 1970 as {@code int}) in the local time
     * zone.
     *
     * <p>Since a time zone is not available, the date is converted to represent the same date as a
     * Unix date in UTC as the {@link java.sql.Date} value in the local time zone.
     *
     * @see #toInt(java.sql.Date, TimeZone)
     * @see #internalToDate(int) converse method
     */
    public static int toInt(java.sql.Date v) {
        return toInt(v, LOCAL_TZ);
    }

    /**
     * Converts a SQL DATE value from the Java type ({@link java.sql.Date}) to the internal
     * representation type (number of days since January 1st, 1970 as {@code int}).
     *
     * <p>The {@link java.sql.Date} class uses the standard Gregorian calendar which switches from
     * the Julian calendar to the Gregorian calendar in October 1582. For compatibility with
     * ISO-8601, the internal representation is converted to use the proleptic Gregorian calendar.
     *
     * <p>If the date contains a partial day, it will be rounded to a full day depending on the
     * milliseconds value. If the milliseconds value is positive, it will be rounded down to the
     * closest full day. If the milliseconds value is negative, it will be rounded up to the closest
     * full day.
     */
    public static int toInt(java.sql.Date v, TimeZone timeZone) {
        return DateTimeUtils.sqlDateToUnixDate(v, timeZone);
    }

    /**
     * Converts a nullable SQL DATE value from the Java type ({@link java.sql.Date}) to the internal
     * representation type (number of days since January 1st, 1970 as {@link Integer}) in the local
     * time zone.
     *
     * <p>Since a time zone is not available, the date is converted to represent the same date as a
     * Unix date in UTC as the {@link java.sql.Date} value in the local time zone.
     *
     * @see #toInt(java.sql.Date, TimeZone)
     * @see #internalToDate(Integer) converse method
     */
    public static Integer toIntOptional(java.sql.Date v) {
        return v == null ? castNonNull(null) : toInt(v);
    }

    /**
     * Converts a nullable SQL DATE value from the Java type ({@link java.sql.Date}) to the internal
     * representation type (number of days since January 1st, 1970 as {@link Integer}).
     *
     * @see #toInt(java.sql.Date, TimeZone)
     */
    public static Integer toIntOptional(java.sql.Date v, TimeZone timeZone) {
        return v == null ? castNonNull(null) : toInt(v, timeZone);
    }

    /**
     * Converts a SQL TIME value from the Java type ({@link java.sql.Time}) to the internal
     * representation type (number of milliseconds since January 1st, 1970 as {@code int}) in the
     * local time zone.
     *
     * @see #toIntOptional(java.sql.Time)
     * @see #internalToTime(int) converse method
     */
    public static int toInt(java.sql.Time v) {
        return DateTimeUtils.sqlTimeToUnixTime(v, LOCAL_TZ);
    }

    /**
     * Converts a nullable SQL TIME value from the Java type ({@link java.sql.Time}) to the internal
     * representation type (number of milliseconds since January 1st, 1970 as {@link Integer}).
     *
     * @see #toInt(java.sql.Time)
     * @see #internalToTime(Integer) converse method
     */
    public static Integer toIntOptional(java.sql.Time v) {
        return v == null ? castNonNull(null) : toInt(v);
    }

    public static int toInt(String s) {
        return Integer.parseInt(s.trim());
    }

    public static int toInt(Number number) {
        return number.intValue();
    }

    public static int toInt(Object o) {
        return o instanceof Integer
                ? (Integer) o
                : o instanceof Number
                        ? toInt((Number) o)
                        : o instanceof String
                                ? toInt((String) o)
                                : o instanceof java.sql.Date
                                        ? toInt((java.sql.Date) o)
                                        : o instanceof java.sql.Time
                                                ? toInt((java.sql.Time) o)
                                                : (Integer) cannotConvert(o, int.class);
    }

    public static Integer toIntOptional(Object o) {
        return o == null ? castNonNull(null) : toInt(o);
    }

    /**
     * Converts a SQL TIMESTAMP value from the Java type ({@link java.util.Date}) to the internal
     * representation type (number of milliseconds since January 1st, 1970 as {@code long}).
     *
     * <p>Since a time zone is not available, converts the timestamp to represent the same date and
     * time as a Unix timestamp in UTC as the {@link java.util.Date} value in the local time zone.
     *
     * <p>The {@link java.util.Date} class uses the standard Gregorian calendar which switches from
     * the Julian calendar to the Gregorian calendar in October 1582. For compatibility with
     * ISO-8601, converts the internal representation to use the proleptic Gregorian calendar.
     */
    public static long toLong(java.util.Date v) {
        return DateTimeUtils.utilDateToUnixTimestamp(v, LOCAL_TZ);
    }

    /**
     * Converts a SQL TIMESTAMP value from the Java type ({@link Timestamp}) to the internal
     * representation type (number of milliseconds since January 1st, 1970 as {@code long}).
     *
     * <p>Since a time zone is not available, converts the timestamp to represent the same date and
     * time as a Unix timestamp in UTC as the {@link Timestamp} value in the local time zone.
     *
     * @see #toLong(Timestamp, TimeZone)
     * @see #internalToTimestamp(Long) converse method
     */
    public static long toLong(Timestamp v) {
        return toLong(v, LOCAL_TZ);
    }

    /**
     * Converts a SQL TIMESTAMP value from the Java type ({@link Timestamp}) to the internal
     * representation type (number of milliseconds since January 1st, 1970 as {@code long}).
     *
     * <p>For backwards compatibility, time zone offsets are calculated in relation to the local
     * time zone instead of UTC. Providing the default time zone or {@code null} will return the
     * timestamp unmodified.
     *
     * <p>The {@link Timestamp} class uses the standard Gregorian calendar which switches from the
     * Julian calendar to the Gregorian calendar in October 1582. For compatibility with ISO-8601,
     * the internal representation is converted to use the proleptic Gregorian calendar.
     */
    public static long toLong(Timestamp v, TimeZone timeZone) {
        return DateTimeUtils.sqlTimestampToUnixTimestamp(v, timeZone);
    }

    /**
     * Converts a nullable SQL TIMESTAMP value from the Java type ({@link Timestamp}) to the
     * internal representation type (number of milliseconds since January 1st, 1970 as {@link Long})
     * in the local time zone.
     *
     * @see #toLong(Timestamp, TimeZone)
     * @see #internalToTimestamp(Long) converse method
     */
    public static Long toLongOptional(Timestamp v) {
        return v == null ? castNonNull(null) : toLong(v, LOCAL_TZ);
    }

    /**
     * Converts a nullable SQL TIMESTAMP value from the Java type ({@link Timestamp}) to the
     * internal representation type (number of milliseconds since January 1st, 1970 as {@link
     * Long}).
     *
     * @see #toLong(Timestamp, TimeZone)
     */
    public static Long toLongOptional(Timestamp v, TimeZone timeZone) {
        if (v == null) {
            return castNonNull(null);
        }
        return toLong(v, timeZone);
    }

    public static long toLong(String s) {
        if (s.startsWith("199") && s.contains(":")) {
            return Timestamp.valueOf(s).getTime();
        }
        return Long.parseLong(s.trim());
    }

    public static long toLong(Number number) {
        return number.longValue();
    }

    public static long toLong(Object o) {
        return o instanceof Long
                ? (Long) o
                : o instanceof Number
                        ? toLong((Number) o)
                        : o instanceof String
                                ? toLong((String) o)
                                : o instanceof java.sql.Date
                                        ? toInt((java.sql.Date) o)
                                        : o instanceof java.sql.Time
                                                ? toInt((java.sql.Time) o)
                                                : o instanceof java.sql.Timestamp
                                                        ? toLong((java.sql.Timestamp) o)
                                                        : o instanceof java.util.Date
                                                                ? toLong((java.util.Date) o)
                                                                : (Long)
                                                                        cannotConvert(
                                                                                o, long.class);
    }

    public static Long toLongOptional(Object o) {
        return o == null ? castNonNull(null) : toLong(o);
    }

    public static float toFloat(String s) {
        return Float.parseFloat(s.trim());
    }

    public static float toFloat(Number number) {
        return number.floatValue();
    }

    public static float toFloat(Object o) {
        return o instanceof Float
                ? (Float) o
                : o instanceof Number
                        ? toFloat((Number) o)
                        : o instanceof String
                                ? toFloat((String) o)
                                : (Float) cannotConvert(o, float.class);
    }

    public static double toDouble(String s) {
        return Double.parseDouble(s.trim());
    }

    public static double toDouble(Number number) {
        return number.doubleValue();
    }

    public static double toDouble(Object o) {
        return o instanceof Double
                ? (Double) o
                : o instanceof Number
                        ? toDouble((Number) o)
                        : o instanceof String
                                ? toDouble((String) o)
                                : (Double) cannotConvert(o, double.class);
    }

    public static BigDecimal toBigDecimal(String s) {
        return new BigDecimal(s.trim());
    }

    public static BigDecimal toBigDecimal(Number number) {
        // There are some values of "long" that cannot be represented as "double".
        // Not so "int". If it isn't a long, go straight to double.
        return number instanceof BigDecimal
                ? (BigDecimal) number
                : number instanceof BigInteger
                        ? new BigDecimal((BigInteger) number)
                        : number instanceof Long
                                ? new BigDecimal(number.longValue())
                                : new BigDecimal(number.doubleValue());
    }

    public static BigDecimal toBigDecimal(Object o) {
        return o instanceof Number ? toBigDecimal((Number) o) : toBigDecimal(o.toString());
    }

    /**
     * Converts a SQL DATE value from the internal representation type (number of days since January
     * 1st, 1970) to the Java type ({@link java.sql.Date}).
     *
     * <p>Since a time zone is not available, converts the date to represent the same date as a
     * {@link java.sql.Date} in the local time zone as the Unix date in UTC.
     *
     * <p>The Unix date should be the number of days since January 1st, 1970 using the proleptic
     * Gregorian calendar as defined by ISO-8601. The returned {@link java.sql.Date} object will use
     * the standard Gregorian calendar which switches from the Julian calendar to the Gregorian
     * calendar in October 1582.
     *
     * @see #internalToDate(Integer)
     * @see #toInt(java.sql.Date) converse method
     */
    public static java.sql.Date internalToDate(int v) {
        final LocalDate date = LocalDate.ofEpochDay(v);
        return java.sql.Date.valueOf(date);
    }

    /**
     * Converts a nullable SQL DATE value from the internal representation type (number of days
     * since January 1st, 1970) to the Java type ({@link java.sql.Date}).
     *
     * @see #internalToDate(int)
     * @see #toIntOptional(java.sql.Date) converse method
     */
    public static java.sql.Date internalToDate(Integer v) {
        return v == null ? castNonNull(null) : internalToDate(v.intValue());
    }

    /**
     * Converts a SQL TIME value from the internal representation type (number of milliseconds since
     * January 1st, 1970) to the Java type ({@link java.sql.Time}).
     *
     * @see #internalToTime(Integer)
     * @see #toInt(java.sql.Time) converse method
     */
    public static java.sql.Time internalToTime(int v) {
        return new java.sql.Time(v - LOCAL_TZ.getOffset(v));
    }

    /**
     * Converts a nullable SQL TIME value from the internal representation type (number of
     * milliseconds since January 1st, 1970) to the Java type ({@link java.sql.Time}).
     *
     * @see #internalToTime(Integer)
     * @see #toIntOptional(java.sql.Time) converse method
     */
    public static java.sql.Time internalToTime(Integer v) {
        return v == null ? castNonNull(null) : internalToTime(v.intValue());
    }

    public static Integer toTimeWithLocalTimeZone(String v) {
        if (v == null) {
            return castNonNull(null);
        }
        return new TimeWithTimeZoneString(v)
                .withTimeZone(DateTimeUtils.UTC_ZONE)
                .getLocalTimeString()
                .getMillisOfDay();
    }

    public static Integer toTimeWithLocalTimeZone(String v, TimeZone timeZone) {
        if (v == null) {
            return castNonNull(null);
        }
        return new TimeWithTimeZoneString(v + " " + timeZone.getID())
                .withTimeZone(DateTimeUtils.UTC_ZONE)
                .getLocalTimeString()
                .getMillisOfDay();
    }

    public static int timeWithLocalTimeZoneToTime(int v, TimeZone timeZone) {
        return TimeWithTimeZoneString.fromMillisOfDay(v)
                .withTimeZone(timeZone)
                .getLocalTimeString()
                .getMillisOfDay();
    }

    public static long timeWithLocalTimeZoneToTimestamp(String date, int v, TimeZone timeZone) {
        final TimeWithTimeZoneString tTZ =
                TimeWithTimeZoneString.fromMillisOfDay(v).withTimeZone(DateTimeUtils.UTC_ZONE);
        return new TimestampWithTimeZoneString(date + " " + tTZ.toString())
                .withTimeZone(timeZone)
                .getLocalTimestampString()
                .getMillisSinceEpoch();
    }

    public static long timeWithLocalTimeZoneToTimestampWithLocalTimeZone(String date, int v) {
        final TimeWithTimeZoneString tTZ =
                TimeWithTimeZoneString.fromMillisOfDay(v).withTimeZone(DateTimeUtils.UTC_ZONE);
        return new TimestampWithTimeZoneString(date + " " + tTZ.toString())
                .getLocalTimestampString()
                .getMillisSinceEpoch();
    }

    public static String timeWithLocalTimeZoneToString(int v, TimeZone timeZone) {
        return TimeWithTimeZoneString.fromMillisOfDay(v).withTimeZone(timeZone).toString();
    }

    /**
     * State for {@code FORMAT_DATE}, {@code FORMAT_TIMESTAMP}, {@code FORMAT_DATETIME}, {@code
     * FORMAT_TIME}, {@code TO_CHAR} functions.
     */
    @Deterministic
    public static class DateFormatFunction {
        /** Work space for various functions. Clear it before you use it. */
        final StringBuilder sb = new StringBuilder();

        /** Cache key. */
        private static class Key extends MapEntry<FormatModel, String> {
            Key(FormatModel formatModel, String format) {
                super(formatModel, format);
            }
        }

        private final LoadingCache<Key, List<FormatElement>> formatCache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(CacheLoader.from(key -> key.t.parseNoCache(key.u)));

        /**
         * Given a format string and a format model, calls an action with the list of elements
         * obtained by parsing that format string.
         */
        protected final void withElements(
                FormatModel formatModel, String format, Consumer<List<FormatElement>> consumer) {
            List<FormatElement> elements = formatCache.getUnchecked(new Key(formatModel, format));
            consumer.accept(elements);
        }

        private String internalFormatDatetime(String fmtString, java.util.Date date) {
            sb.setLength(0);
            withElements(
                    FormatModels.BIG_QUERY,
                    fmtString,
                    elements -> elements.forEach(element -> element.format(sb, date)));
            return sb.toString();
        }

        public String formatTimestamp(DataContext ctx, String fmtString, long timestamp) {
            return internalFormatDatetime(fmtString, internalToTimestamp(timestamp));
        }

        public String toChar(long timestamp, String pattern) {
            final Timestamp sqlTimestamp = internalToTimestamp(timestamp);
            sb.setLength(0);
            withElements(
                    FormatModels.POSTGRESQL,
                    pattern,
                    elements -> elements.forEach(element -> element.format(sb, sqlTimestamp)));
            return sb.toString().trim();
        }

        public String formatDate(DataContext ctx, String fmtString, int date) {
            return internalFormatDatetime(fmtString, internalToDate(date));
        }

        public String formatTime(DataContext ctx, String fmtString, int time) {
            return internalFormatDatetime(fmtString, internalToTime(time));
        }
    }

    /**
     * State for {@code PARSE_DATE}, {@code PARSE_TIMESTAMP}, {@code PARSE_DATETIME}, {@code
     * PARSE_TIME} functions.
     */
    @Deterministic
    public static class DateParseFunction {
        /** Use a {@link DateFormatFunction} for its cache of parsed format strings. */
        final DateFormatFunction f = new DateFormatFunction();

        private final LoadingCache<Key, DateFormat> cache =
                CacheBuilder.newBuilder()
                        .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
                        .build(CacheLoader.from(key -> key.toDateFormat(f)));

        private <T> T withParser(String fmt, String timeZone, Function<DateFormat, T> action) {
            final DateFormat dateFormat = cache.getUnchecked(new Key(fmt, timeZone));
            return action.apply(dateFormat);
        }

        private long internalParseDatetime(String fmtString, String datetime) {
            return internalParseDatetime(fmtString, datetime, DateTimeUtils.DEFAULT_ZONE.getID());
        }

        private long internalParseDatetime(String fmt, String datetime, String timeZone) {
            final ParsePosition pos = new ParsePosition(0);
            Date parsed = withParser(fmt, timeZone, parser -> parser.parse(datetime, pos));
            // Throw if either the parse was unsuccessful, or the format string did
            // not contain enough elements to parse the datetime string completely.
            if (pos.getErrorIndex() >= 0 || pos.getIndex() != datetime.length()) {
                SQLException e =
                        new SQLException(
                                String.format(
                                        Locale.ROOT,
                                        "Invalid format: '%s' for datetime string: '%s'.",
                                        fmt,
                                        datetime));
                throw Util.toUnchecked(e);
            }
            // Suppress the Errorprone warning "[JavaUtilDate] Date has a bad API that
            // leads to bugs; prefer java.time.Instant or LocalDate" because we know
            // what we're doing.
            @SuppressWarnings("JavaUtilDate")
            final long millisSinceEpoch = parsed.getTime();
            return millisSinceEpoch;
        }

        public int parseDate(String fmtString, String date) {
            final long millisSinceEpoch = internalParseDatetime(fmtString, date);
            return toInt(new java.sql.Date(millisSinceEpoch));
        }

        public long parseDatetime(String fmtString, String datetime) {
            final long millisSinceEpoch = internalParseDatetime(fmtString, datetime);
            return toLong(new Timestamp(millisSinceEpoch));
        }

        public int parseTime(String fmtString, String time) {
            final long millisSinceEpoch = internalParseDatetime(fmtString, time);
            return toInt(new Time(millisSinceEpoch));
        }

        public long parseTimestamp(String fmtString, String timestamp) {
            return parseTimestamp(fmtString, timestamp, "UTC");
        }

        public long parseTimestamp(String fmtString, String timestamp, String timeZone) {
            TimeZone tz = TimeZone.getTimeZone(timeZone);
            final long millisSinceEpoch = internalParseDatetime(fmtString, timestamp, timeZone);
            return toLong(new java.sql.Timestamp(millisSinceEpoch), tz);
        }

        /** Key for cache of parsed format strings. */
        private static final class Key {
            final String fmt;
            final String timeZone;

            Key(String fmt, String timeZone) {
                this.fmt = fmt;
                this.timeZone = timeZone;
            }

            @Override
            public int hashCode() {
                return fmt.hashCode() + timeZone.hashCode() * 37;
            }

            @Override
            public boolean equals(@Nullable Object obj) {
                return this == obj
                        || obj instanceof Key
                                && fmt.equals(((Key) obj).fmt)
                                && timeZone.equals(((Key) obj).timeZone);
            }

            DateFormat toDateFormat(DateFormatFunction f) {
                f.sb.setLength(0);
                f.withElements(
                        FormatModels.BIG_QUERY,
                        fmt,
                        elements -> elements.forEach(ele -> ele.toPattern(f.sb)));
                final String javaFmt = f.sb.toString();

                // TODO: make Locale configurable. ENGLISH set for weekday
                // parsing (e.g. Thursday, Friday).
                final DateFormat parser = new SimpleDateFormat(javaFmt, Locale.ENGLISH);
                parser.setLenient(false);
                TimeZone tz = TimeZone.getTimeZone(timeZone);
                parser.setCalendar(Calendar.getInstance(tz, Locale.ROOT));
                return parser;
            }
        }
    }

    /**
     * Converts a SQL TIMESTAMP value from the internal representation type (number of milliseconds
     * since January 1st, 1970) to the Java Type ({@link Timestamp}) in the local time zone.
     *
     * <p>Since a time zone is not available, the timestamp is converted to represent the same
     * timestamp as a {@link Timestamp} in the local time zone as the Unix timestamp in UTC.
     *
     * <p>The Unix timestamp should be the number of milliseconds since January 1st, 1970 using the
     * proleptic Gregorian calendar as defined by ISO-8601. The returned {@link Timestamp} object
     * will use the standard Gregorian calendar which switches from the Julian calendar to the
     * Gregorian calendar in October 1582.
     *
     * @see #internalToTimestamp(Long)
     * @see #toLong(Timestamp, TimeZone)
     * @see #toLongOptional(Timestamp)
     * @see #toLongOptional(Timestamp, TimeZone)
     * @see #toLong(Timestamp) converse method
     */
    public static java.sql.Timestamp internalToTimestamp(long v) {
        final LocalDateTime dateTime =
                LocalDateTime.ofEpochSecond(
                        Math.floorDiv(v, DateTimeUtils.MILLIS_PER_SECOND),
                        (int)
                                (Math.floorMod(v, DateTimeUtils.MILLIS_PER_SECOND)
                                        * DateTimeUtils.NANOS_PER_MILLI),
                        ZoneOffset.UTC);
        return java.sql.Timestamp.valueOf(dateTime);
    }

    /**
     * Converts a nullable SQL TIMESTAMP value from the internal representation type (number of
     * milliseconds since January 1st, 1970) to the Java Type ({@link Timestamp}) in the local time
     * zone.
     *
     * @see #internalToTimestamp(long)
     * @see #toLong(Timestamp)
     * @see #toLong(Timestamp, TimeZone)
     * @see #toLongOptional(Timestamp, TimeZone)
     * @see #toLongOptional(Timestamp) converse method
     */
    public static java.sql.Timestamp internalToTimestamp(Long v) {
        return v == null ? castNonNull(null) : internalToTimestamp(v.longValue());
    }

    public static int timestampWithLocalTimeZoneToDate(long v, TimeZone timeZone) {
        return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
                .withTimeZone(timeZone)
                .getLocalDateString()
                .getDaysSinceEpoch();
    }

    public static int timestampWithLocalTimeZoneToTime(long v, TimeZone timeZone) {
        return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
                .withTimeZone(timeZone)
                .getLocalTimeString()
                .getMillisOfDay();
    }

    public static long timestampWithLocalTimeZoneToTimestamp(long v, TimeZone timeZone) {
        return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
                .withTimeZone(timeZone)
                .getLocalTimestampString()
                .getMillisSinceEpoch();
    }

    public static String timestampWithLocalTimeZoneToString(long v, TimeZone timeZone) {
        return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
                .withTimeZone(timeZone)
                .toString();
    }

    public static int timestampWithLocalTimeZoneToTimeWithLocalTimeZone(long v) {
        return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
                .getLocalTimeString()
                .getMillisOfDay();
    }

    /** For {@link SqlLibraryOperators#TIMESTAMP_SECONDS}. */
    public static long timestampSeconds(long v) {
        return v * 1000;
    }

    /** For {@link SqlLibraryOperators#TIMESTAMP_MILLIS}. */
    public static long timestampMillis(long v) {
        // translation is trivial, because Calcite represents TIMESTAMP values as
        // millis since epoch
        return v;
    }

    /** For {@link SqlLibraryOperators#TIMESTAMP_MICROS}. */
    public static long timestampMicros(long v) {
        return v / 1000;
    }

    /** For {@link SqlLibraryOperators#UNIX_SECONDS}. */
    public static long unixSeconds(long v) {
        return v / 1000;
    }

    /** For {@link SqlLibraryOperators#UNIX_MILLIS}. */
    public static long unixMillis(long v) {
        // translation is trivial, because Calcite represents TIMESTAMP values as
        // millis since epoch
        return v;
    }

    /** For {@link SqlLibraryOperators#UNIX_MICROS}. */
    public static long unixMicros(long v) {
        return v * 1000;
    }

    /** For {@link SqlLibraryOperators#DATE_FROM_UNIX_DATE}. */
    public static int dateFromUnixDate(int v) {
        // translation is trivial, because Calcite represents dates as Unix integers
        return v;
    }

    /** For {@link SqlLibraryOperators#UNIX_DATE}. */
    public static int unixDate(int v) {
        // translation is trivial, because Calcite represents dates as Unix integers
        return v;
    }

    /** SQL {@code DATE(year, month, day)} function. */
    public static int date(int year, int month, int day) {
        // Calcite represents dates as Unix integers (days since epoch).
        return (int) LocalDate.of(year, month, day).toEpochDay();
    }

    /** SQL {@code DATE(TIMESTAMP)} and {@code DATE(TIMESTAMP WITH LOCAL TIME ZONE)} functions. */
    public static int date(long timestampMillis) {
        // Calcite represents dates as Unix integers (days since epoch).
        // Unix time ignores leap seconds; every day has the exact same number of
        // milliseconds. BigQuery TIMESTAMP and DATETIME values (Calcite TIMESTAMP
        // WITH LOCAL TIME ZONE and TIMESTAMP, respectively) are represented
        // internally as milliseconds since epoch (or epoch UTC).
        return (int) (timestampMillis / DateTimeUtils.MILLIS_PER_DAY);
    }

    /** SQL {@code DATE(TIMESTAMP WITH LOCAL TIME, timeZone)} function. */
    public static int date(long timestampMillis, String timeZone) {
        // Calcite represents dates as Unix integers (days since epoch).
        return (int)
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), ZoneId.of(timeZone))
                        .toLocalDate()
                        .toEpochDay();
    }

    /** SQL {@code DATETIME(<year>, <month>, <day>, <hour>, <minute>, <second>)} function. */
    public static long datetime(int year, int month, int day, int hour, int minute, int second) {
        // BigQuery's DATETIME function returns a Calcite TIMESTAMP,
        // represented internally as milliseconds since epoch UTC.
        return LocalDateTime.of(year, month, day, hour, minute, second)
                        .toEpochSecond(ZoneOffset.UTC)
                * DateTimeUtils.MILLIS_PER_SECOND;
    }

    /** SQL {@code DATETIME(DATE)} function; returns a Calcite TIMESTAMP. */
    public static long datetime(int daysSinceEpoch) {
        // BigQuery's DATETIME function returns a Calcite TIMESTAMP,
        // represented internally as milliseconds since epoch.
        return daysSinceEpoch * DateTimeUtils.MILLIS_PER_DAY;
    }

    /** SQL {@code DATETIME(DATE, TIME)} function; returns a Calcite TIMESTAMP. */
    public static long datetime(int daysSinceEpoch, int millisSinceMidnight) {
        // BigQuery's DATETIME function returns a Calcite TIMESTAMP,
        // represented internally as milliseconds since epoch UTC.
        return daysSinceEpoch * DateTimeUtils.MILLIS_PER_DAY + millisSinceMidnight;
    }

    /**
     * SQL {@code DATETIME(TIMESTAMP WITH LOCAL TIME ZONE)} function; returns a Calcite TIMESTAMP.
     */
    public static long datetime(long millisSinceEpoch) {
        // BigQuery TIMESTAMP and DATETIME values (Calcite TIMESTAMP WITH LOCAL TIME
        // ZONE and TIMESTAMP, respectively) are represented internally as
        // milliseconds since epoch (or epoch UTC).
        return millisSinceEpoch;
    }

    /** SQL {@code DATETIME(TIMESTAMP, timeZone)} function; returns a Calcite TIMESTAMP. */
    public static long datetime(long millisSinceEpoch, String timeZone) {
        // BigQuery TIMESTAMP and DATETIME values (Calcite TIMESTAMP WITH LOCAL TIME
        // ZONE and TIMESTAMP, respectively) are represented internally as
        // milliseconds since epoch (or epoch UTC).
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millisSinceEpoch), ZoneId.of(timeZone))
                .atZoneSimilarLocal(ZoneId.of("UTC"))
                .toInstant()
                .toEpochMilli();
    }

    /** SQL {@code TIMESTAMP(<string>)} function. */
    public static long timestamp(String expression) {
        // Calcite represents TIMESTAMP WITH LOCAL TIME ZONE as Unix integers
        // (milliseconds since epoch).
        return parseBigQueryTimestampLiteral(expression).toInstant().toEpochMilli();
    }

    /** SQL {@code TIMESTAMP(<string>, <timeZone>)} function. */
    public static long timestamp(String expression, String timeZone) {
        // Calcite represents TIMESTAMP WITH LOCAL TIME ZONE as Unix integers
        // (milliseconds since epoch).
        return parseBigQueryTimestampLiteral(expression)
                .atZoneSimilarLocal(ZoneId.of(timeZone))
                .toInstant()
                .toEpochMilli();
    }

    private static OffsetDateTime parseBigQueryTimestampLiteral(String expression) {
        // First try to parse with an offset, otherwise parse as a local and assume
        // UTC ("no offset").
        try {
            return OffsetDateTime.parse(expression, BIG_QUERY_TIMESTAMP_LITERAL_FORMATTER);
        } catch (DateTimeParseException e) {
            // ignore
        }
        if (IS_JDK_8 && expression.matches(".*[+-][0-9][0-9]$")) {
            // JDK 8 has a bug that prevents matching offsets like "+00" but can
            // match "+00:00".
            try {
                expression += ":00";
                return OffsetDateTime.parse(expression, BIG_QUERY_TIMESTAMP_LITERAL_FORMATTER);
            } catch (DateTimeParseException e) {
                // ignore
            }
        }
        try {
            return LocalDateTime.parse(expression, BIG_QUERY_TIMESTAMP_LITERAL_FORMATTER)
                    .atOffset(ZoneOffset.UTC);
        } catch (DateTimeParseException e2) {
            throw new IllegalArgumentException(
                    String.format(
                            Locale.ROOT,
                            "Could not parse BigQuery timestamp literal: %s",
                            expression),
                    e2);
        }
    }

    /** SQL {@code TIMESTAMP(<date>)} function. */
    public static long timestamp(int days) {
        // Calcite represents TIMESTAMP WITH LOCAL TIME ZONE as Unix integers
        // (milliseconds since epoch). Unix time ignores leap seconds; every day
        // has the same number of milliseconds.
        return ((long) days) * DateTimeUtils.MILLIS_PER_DAY;
    }

    /** SQL {@code TIMESTAMP(<date>, <timeZone>)} function. */
    public static long timestamp(int days, String timeZone) {
        // Calcite represents TIMESTAMP WITH LOCAL TIME ZONE as Unix integers
        // (milliseconds since epoch).
        final LocalDateTime localDateTime =
                LocalDateTime.of(LocalDate.ofEpochDay(days), LocalTime.MIDNIGHT);
        final ZoneOffset zoneOffset = ZoneId.of(timeZone).getRules().getOffset(localDateTime);
        return OffsetDateTime.of(localDateTime, zoneOffset).toInstant().toEpochMilli();
    }

    /** SQL {@code TIMESTAMP(<timestamp>)} function; returns a TIMESTAMP WITH LOCAL TIME ZONE. */
    public static long timestamp(long millisSinceEpoch) {
        // BigQuery TIMESTAMP and DATETIME values (Calcite TIMESTAMP WITH LOCAL
        // TIME ZONE and TIMESTAMP, respectively) are represented internally as
        // milliseconds since epoch UTC and epoch.
        return millisSinceEpoch;
    }

    /**
     * SQL {@code TIMESTAMP(<timestamp>, <timeZone>)} function; returns a TIMESTAMP WITH LOCAL TIME
     * ZONE.
     */
    public static long timestamp(long millisSinceEpoch, String timeZone) {
        // BigQuery TIMESTAMP and DATETIME values (Calcite TIMESTAMP WITH LOCAL
        // TIME ZONE and TIMESTAMP, respectively) are represented internally as
        // milliseconds since epoch UTC and epoch.
        final Instant instant = Instant.ofEpochMilli(millisSinceEpoch);
        final ZoneId utcZone = ZoneId.of("UTC");
        return OffsetDateTime.ofInstant(instant, utcZone)
                .atZoneSimilarLocal(ZoneId.of(timeZone))
                .toInstant()
                .toEpochMilli();
    }

    /** SQL {@code TIME(<hour>, <minute>, <second>)} function. */
    public static int time(int hour, int minute, int second) {
        // Calcite represents time as Unix integers (milliseconds since midnight).
        return (int)
                (LocalTime.of(hour, minute, second).toSecondOfDay()
                        * DateTimeUtils.MILLIS_PER_SECOND);
    }

    /** SQL {@code TIME(<timestamp>)} and {@code TIME(<timestampLtz>)} functions. */
    public static int time(long timestampMillis) {
        // Calcite represents time as Unix integers (milliseconds since midnight).
        // Unix time ignores leap seconds; every day has the same number of
        // milliseconds.
        //
        // BigQuery TIMESTAMP and DATETIME values (Calcite TIMESTAMP WITH LOCAL
        // TIME ZONE and TIMESTAMP, respectively) are represented internally as
        // milliseconds since epoch UTC and epoch.
        return (int) (timestampMillis % DateTimeUtils.MILLIS_PER_DAY);
    }

    /** SQL {@code TIME(<timestampLtz>, <timeZone>)} function. */
    public static int time(long timestampMillis, String timeZone) {
        // Calcite represents time as Unix integers (milliseconds since midnight).
        // Unix time ignores leap seconds; every day has the same number of
        // milliseconds.
        final Instant instant = Instant.ofEpochMilli(timestampMillis);
        final ZoneId zoneId = ZoneId.of(timeZone);
        return (int)
                (OffsetDateTime.ofInstant(instant, zoneId).toLocalTime().toNanoOfDay()
                        / (1000L * 1000L)); // milli > micro > nano
    }

    public static Long toTimestampWithLocalTimeZone(String v) {
        if (v == null) {
            return castNonNull(null);
        }
        return new TimestampWithTimeZoneString(v)
                .withTimeZone(DateTimeUtils.UTC_ZONE)
                .getLocalTimestampString()
                .getMillisSinceEpoch();
    }

    public static Long toTimestampWithLocalTimeZone(String v, TimeZone timeZone) {
        if (v == null) {
            return castNonNull(null);
        }
        return new TimestampWithTimeZoneString(v + " " + timeZone.getID())
                .withTimeZone(DateTimeUtils.UTC_ZONE)
                .getLocalTimestampString()
                .getMillisSinceEpoch();
    }

    // Don't need shortValueOf etc. - Short.valueOf is sufficient.

    /** Helper for CAST(... AS VARCHAR(maxLength)). */
    public static String truncate(String s, int maxLength) {
        if (s == null) {
            return s;
        } else if (s.length() > maxLength) {
            return s.substring(0, maxLength);
        } else {
            return s;
        }
    }

    /** Helper for CAST(... AS CHAR(maxLength)). */
    public static String truncateOrPad(String s, int maxLength) {
        if (s == null) {
            return s;
        } else {
            final int length = s.length();
            if (length > maxLength) {
                return s.substring(0, maxLength);
            } else {
                return length < maxLength ? Spaces.padRight(s, maxLength) : s;
            }
        }
    }

    /** Helper for CAST(... AS VARBINARY(maxLength)). */
    public static ByteString truncate(ByteString s, int maxLength) {
        if (s == null) {
            return s;
        } else if (s.length() > maxLength) {
            return s.substring(0, maxLength);
        } else {
            return s;
        }
    }

    /** Helper for CAST(... AS BINARY(maxLength)). */
    public static ByteString truncateOrPad(ByteString s, int maxLength) {
        if (s == null) {
            return s;
        } else {
            final int length = s.length();
            if (length > maxLength) {
                return s.substring(0, maxLength);
            } else if (length < maxLength) {
                return s.concat(new ByteString(new byte[maxLength - length]));
            } else {
                return s;
            }
        }
    }

    /** SQL {@code POSITION(seek IN string)} function. */
    public static int position(String seek, String s) {
        return s.indexOf(seek) + 1;
    }

    /** SQL {@code POSITION(seek IN string)} function for byte strings. */
    public static int position(ByteString seek, ByteString s) {
        return s.indexOf(seek) + 1;
    }

    /** SQL {@code POSITION(seek IN string FROM integer)} function. */
    public static int position(String seek, String s, int from) {
        if (from == 0) {
            throw RESOURCE.fromNotZero().ex();
        }
        if (from > 0) {
            return positionForwards(seek, s, from);
        } else {
            from += s.length(); // convert negative position to positive index
            return positionBackwards(seek, s, from);
        }
    }

    /** SQL {@code POSITION(seek IN string FROM integer)} function for byte strings. */
    public static int position(ByteString seek, ByteString s, int from) {
        if (from == 0) {
            throw RESOURCE.fromNotZero().ex();
        }
        if (from > 0) {
            return positionForwards(seek, s, from);
        } else {
            from += s.length(); // convert negative position to 0-based index
            return positionBackwards(seek, s, from);
        }
    }

    /**
     * Returns the position (1-based) of {@code seek} in string {@code s} seeking forwards from
     * {@code from} (1-based).
     */
    private static int positionForwards(String seek, String s, int from) {
        final int from0 = from - 1; // 0-based
        if (from0 >= s.length()) {
            return 0;
        } else {
            return s.indexOf(seek, from0) + 1;
        }
    }

    /**
     * Returns the position (1-based) of {@code seek} in byte string {@code s} seeking forwards from
     * {@code from} (1-based).
     */
    private static int positionForwards(ByteString seek, ByteString s, int from) {
        final int from0 = from - 1; // 0-based
        if (from0 >= s.length()) {
            return 0;
        } else {
            return s.indexOf(seek, from0) + 1;
        }
    }

    /**
     * Returns the position (1-based) of {@code seek} in string {@code s} seeking backwards from
     * {@code rightIndex} (0-based).
     */
    private static int positionBackwards(String seek, String s, int rightIndex) {
        if (rightIndex <= 0) {
            return 0;
        }
        int lastIndex = s.lastIndexOf(seek) + 1;
        while (lastIndex > rightIndex + 1) {
            lastIndex = s.substring(0, lastIndex - 1).lastIndexOf(seek) + 1;
            if (lastIndex == 0) {
                return 0;
            }
        }
        return lastIndex;
    }

    /**
     * Returns the position (1-based) of {@code seek} in byte string {@code s} seeking backwards
     * from {@code rightIndex} (0-based).
     */
    private static int positionBackwards(ByteString seek, ByteString s, int rightIndex) {
        if (rightIndex <= 0) {
            return 0;
        }
        int lastIndex = 0;
        while (lastIndex < rightIndex) {
            // NOTE: When [CALCITE-5682] is fixed, use ByteString.lastIndexOf
            int indexOf = s.substring(lastIndex).indexOf(seek) + 1;
            if (indexOf == 0 || lastIndex + indexOf > rightIndex + 1) {
                break;
            }
            lastIndex += indexOf;
        }
        return lastIndex;
    }

    /** SQL {@code POSITION(seek, string, from, occurrence)} function. */
    public static int position(String seek, String s, int from, int occurrence) {
        if (from == 0) {
            throw RESOURCE.fromNotZero().ex();
        }
        if (occurrence == 0) {
            throw RESOURCE.occurrenceNotZero().ex();
        }
        if (from > 0) {
            // Forwards
            --from; // compensate for the '++from' 2 lines down
            for (int i = 0; i < occurrence; i++) {
                ++from; // move on to next occurrence
                from = positionForwards(seek, s, from);
                if (from == 0) {
                    return 0;
                }
            }
        } else {
            // Backwards
            from += s.length() + 1; // convert negative position to positive index
            ++from; // compensate for the '--from' 2 lines down
            for (int i = 0; i < occurrence; i++) {
                --from; // move on to next occurrence
                from = positionBackwards(seek, s, from - 1);
                if (from == 0) {
                    return 0;
                }
            }
        }
        return from;
    }

    /** SQL {@code POSITION(seek, string, from, occurrence)} function for byte strings. */
    public static int position(ByteString seek, ByteString s, int from, int occurrence) {
        if (from == 0) {
            throw RESOURCE.fromNotZero().ex();
        }
        if (occurrence == 0) {
            throw RESOURCE.occurrenceNotZero().ex();
        }
        if (from > 0) {
            // Forwards
            --from; // compensate for the '++from' 2 lines down
            for (int i = 0; i < occurrence; i++) {
                ++from; // move on to next occurrence
                from = positionForwards(seek, s, from);
                if (from == 0) {
                    return 0;
                }
            }
        } else {
            // Backwards
            from += s.length() + 1; // convert negative position to positive index
            ++from; // compensate for the '--from' 2 lines down
            for (int i = 0; i < occurrence; i++) {
                --from; // move on to next occurrence
                from = positionBackwards(seek, s, from - 1);
                if (from == 0) {
                    return 0;
                }
            }
        }
        return from;
    }

    /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
    public static long round(long v, long x) {
        return truncate(v + x / 2, x);
    }

    /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
    public static long truncate(long v, long x) {
        long remainder = v % x;
        if (remainder < 0) {
            remainder += x;
        }
        return v - remainder;
    }

    /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
    public static int round(int v, int x) {
        return truncate(v + x / 2, x);
    }

    /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
    public static int truncate(int v, int x) {
        int remainder = v % x;
        if (remainder < 0) {
            remainder += x;
        }
        return v - remainder;
    }

    /**
     * SQL {@code DAYNAME} function, applied to a TIMESTAMP argument.
     *
     * @param timestamp Milliseconds from epoch
     * @param locale Locale
     * @return Name of the weekday in the given locale
     */
    public static String dayNameWithTimestamp(long timestamp, Locale locale) {
        return timeStampToLocalDate(timestamp).format(ROOT_DAY_FORMAT.withLocale(locale));
    }

    /**
     * SQL {@code DAYNAME} function, applied to a DATE argument.
     *
     * @param date Days since epoch
     * @param locale Locale
     * @return Name of the weekday in the given locale
     */
    public static String dayNameWithDate(int date, Locale locale) {
        return dateToLocalDate(date).format(ROOT_DAY_FORMAT.withLocale(locale));
    }

    /**
     * SQL {@code MONTHNAME} function, applied to a TIMESTAMP argument.
     *
     * @param timestamp Milliseconds from epoch
     * @param locale Locale
     * @return Name of the month in the given locale
     */
    public static String monthNameWithTimestamp(long timestamp, Locale locale) {
        return timeStampToLocalDate(timestamp).format(ROOT_MONTH_FORMAT.withLocale(locale));
    }

    /**
     * SQL {@code MONTHNAME} function, applied to a DATE argument.
     *
     * @param date Days from epoch
     * @param locale Locale
     * @return Name of the month in the given locale
     */
    public static String monthNameWithDate(int date, Locale locale) {
        return dateToLocalDate(date).format(ROOT_MONTH_FORMAT.withLocale(locale));
    }

    /**
     * Converts a date (days since epoch) to a {@link LocalDate}.
     *
     * @param date days since epoch
     * @return localDate
     */
    private static LocalDate dateToLocalDate(int date) {
        int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
        int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
        int d0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.DAY, date);
        return LocalDate.of(y0, m0, d0);
    }

    /**
     * Converts a timestamp (milliseconds since epoch) to a {@link LocalDate}.
     *
     * @param timestamp milliseconds from epoch
     * @return localDate
     */
    private static LocalDate timeStampToLocalDate(long timestamp) {
        int date = timestampToDate(timestamp);
        return dateToLocalDate(date);
    }

    /** Converts a timestamp (milliseconds since epoch) to a date (days since epoch). */
    public static int timestampToDate(long timestamp) {
        return (int) (timestamp / DateTimeUtils.MILLIS_PER_DAY);
    }

    /** Converts a timestamp (milliseconds since epoch) to a time (milliseconds since midnight). */
    public static int timestampToTime(long timestamp) {
        return (int) (timestamp % DateTimeUtils.MILLIS_PER_DAY);
    }

    /** SQL {@code CURRENT_TIMESTAMP} function. */
    @NonDeterministic
    public static long currentTimestamp(DataContext root) {
        return DataContext.Variable.CURRENT_TIMESTAMP.get(root);
    }

    /** SQL {@code CURRENT_TIME} function. */
    @NonDeterministic
    public static int currentTime(DataContext root) {
        int time = timestampToTime(currentTimestamp(root));
        if (time < 0) {
            time = (int) (time + DateTimeUtils.MILLIS_PER_DAY);
        }
        return time;
    }

    /** SQL {@code CURRENT_DATE} function. */
    @NonDeterministic
    public static int currentDate(DataContext root) {
        final long timestamp = currentTimestamp(root);
        int date = timestampToDate(timestamp);
        final int time = timestampToTime(timestamp);
        if (time < 0) {
            --date;
        }
        return date;
    }

    /** SQL {@code CURRENT_DATETIME} function. */
    @NonDeterministic
    public static Long currentDatetime(DataContext root) {
        final long timestamp = DataContext.Variable.CURRENT_TIMESTAMP.get(root);
        return datetime(timestamp);
    }

    /** SQL {@code CURRENT_DATETIME} function with a specified timezone. */
    @NonDeterministic
    public static @Nullable Long currentDatetime(DataContext root, @Nullable String timezone) {
        if (timezone == null) {
            return null;
        }
        final long timestamp = DataContext.Variable.UTC_TIMESTAMP.get(root);
        return datetime(timestamp, timezone);
    }

    /** SQL {@code LOCAL_TIMESTAMP} function. */
    @NonDeterministic
    public static long localTimestamp(DataContext root) {
        return DataContext.Variable.LOCAL_TIMESTAMP.get(root);
    }

    /** SQL {@code LOCAL_TIME} function. */
    @NonDeterministic
    public static int localTime(DataContext root) {
        return timestampToTime(localTimestamp(root));
    }

    @NonDeterministic
    public static TimeZone timeZone(DataContext root) {
        return DataContext.Variable.TIME_ZONE.get(root);
    }

    /** SQL {@code USER} function. */
    @Deterministic
    public static String user(DataContext root) {
        return requireNonNull(DataContext.Variable.USER.get(root));
    }

    /** SQL {@code SYSTEM_USER} function. */
    @Deterministic
    public static String systemUser(DataContext root) {
        return requireNonNull(DataContext.Variable.SYSTEM_USER.get(root));
    }

    @NonDeterministic
    public static Locale locale(DataContext root) {
        return DataContext.Variable.LOCALE.get(root);
    }

    /**
     * SQL {@code DATEADD} function applied to a custom time frame.
     *
     * <p>Custom time frames are created as part of a {@link TimeFrameSet}. This method retrieves
     * the session's time frame set from the {@link DataContext.Variable#TIME_FRAME_SET} variable,
     * then looks up the time frame by name.
     */
    public static int customDateAdd(
            DataContext root, String timeFrameName, int interval, int date) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.addDate(date, interval, timeFrame);
    }

    /**
     * SQL {@code TIMESTAMPADD} function applied to a custom time frame.
     *
     * <p>Custom time frames are created and accessed as described in {@link #customDateAdd}.
     */
    public static long customTimestampAdd(
            DataContext root, String timeFrameName, long interval, long timestamp) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.addTimestamp(timestamp, interval, timeFrame);
    }

    /**
     * SQL {@code DATEDIFF} function applied to a custom time frame.
     *
     * <p>Custom time frames are created and accessed as described in {@link #customDateAdd}.
     */
    public static int customDateDiff(DataContext root, String timeFrameName, int date, int date2) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.diffDate(date, date2, timeFrame);
    }

    /**
     * SQL {@code TIMESTAMPDIFF} function applied to a custom time frame.
     *
     * <p>Custom time frames are created and accessed as described in {@link #customDateAdd}.
     */
    public static long customTimestampDiff(
            DataContext root, String timeFrameName, long timestamp, long timestamp2) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.diffTimestamp(timestamp, timestamp2, timeFrame);
    }

    /**
     * SQL {@code FLOOR} function applied to a {@code DATE} value and a custom time frame.
     *
     * <p>Custom time frames are created and accessed as described in {@link #customDateAdd}.
     */
    public static int customDateFloor(DataContext root, String timeFrameName, int date) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.floorDate(date, timeFrame);
    }

    /**
     * SQL {@code CEIL} function applied to a {@code DATE} value and a custom time frame.
     *
     * <p>Custom time frames are created and accessed as described in {@link #customDateAdd}.
     */
    public static int customDateCeil(DataContext root, String timeFrameName, int date) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.ceilDate(date, timeFrame);
    }

    /**
     * SQL {@code FLOOR} function applied to a {@code TIMESTAMP} value and a custom time frame.
     *
     * <p>Custom time frames are created and accessed as described in {@link #customDateAdd}.
     */
    public static long customTimestampFloor(
            DataContext root, String timeFrameName, long timestamp) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.floorTimestamp(timestamp, timeFrame);
    }

    /**
     * SQL {@code CEIL} function applied to a {@code TIMESTAMP} value and a custom time frame.
     *
     * <p>Custom time frames are created and accessed as described in {@link #customDateAdd}.
     */
    public static long customTimestampCeil(DataContext root, String timeFrameName, long timestamp) {
        final TimeFrameSet timeFrameSet =
                requireNonNull(DataContext.Variable.TIME_FRAME_SET.get(root));
        final TimeFrame timeFrame = timeFrameSet.get(timeFrameName);
        return timeFrameSet.ceilTimestamp(timestamp, timeFrame);
    }

    /** SQL {@code TRANSLATE(string, search_chars, replacement_chars)} function. */
    public static String translate3(String s, String search, String replacement) {
        return org.apache.commons.lang3.StringUtils.replaceChars(s, search, replacement);
    }

    /** SQL {@code REPLACE(string, search, replacement)} function. */
    public static String replace(String s, String search, String replacement) {
        return s.replace(search, replacement);
    }

    /**
     * Helper for "array element reference". Caller has already ensured that array and index are not
     * null.
     *
     * <p>Index may be 0- or 1-based depending on which array subscript operator is being used.
     * {@code ITEM}, {@code ORDINAL}, and {@code SAFE_ORDINAL} are 1-based, while {@code OFFSET} and
     * {@code SAFE_OFFSET} are 0-based.
     *
     * <p>The {@code ITEM}, {@code SAFE_OFFSET}, and {@code SAFE_ORDINAL} operators return null if
     * the index is out of bounds, while the others throw an error.
     */
    public static @Nullable Object arrayItem(List list, int item, int offset, boolean safe) {
        if (item < offset || item > list.size() + 1 - offset) {
            if (safe) {
                return null;
            } else {
                throw RESOURCE.arrayIndexOutOfBounds(item).ex();
            }
        }
        return list.get(item - offset);
    }

    /**
     * Helper for "map element reference". Caller has already ensured that array and index are not
     * null. Index is 1-based, per SQL.
     */
    public static @Nullable Object mapItem(Map map, Object item) {
        return map.get(item);
    }

    /**
     * Implements the {@code [ ... ]} operator on an object whose type is not known until runtime.
     */
    public static @Nullable Object item(Object object, Object index) {
        if (object instanceof Map) {
            return mapItem((Map) object, index);
        }
        if (object instanceof List && index instanceof Number) {
            return arrayItem((List) object, ((Number) index).intValue(), 1, true);
        }
        if (index instanceof Number) {
            return structAccess(object, ((Number) index).intValue() - 1, null); // 1 indexed
        }
        if (index instanceof String) {
            return structAccess(object, -1, index.toString());
        }

        return null;
    }

    /** As {@link #arrayItem} method, but allows array to be nullable. */
    public static @Nullable Object arrayItemOptional(
            @Nullable List list, int item, int offset, boolean safe) {
        if (list == null) {
            return null;
        }
        return arrayItem(list, item, offset, safe);
    }

    /** As {@link #mapItem} method, but allows map to be nullable. */
    public static @Nullable Object mapItemOptional(@Nullable Map map, Object item) {
        if (map == null) {
            return null;
        }
        return mapItem(map, item);
    }

    /** As {@link #item} method, but allows object to be nullable. */
    public static @Nullable Object itemOptional(@Nullable Object object, Object index) {
        if (object == null) {
            return null;
        }
        return item(object, index);
    }

    /** NULL &rarr; FALSE, FALSE &rarr; FALSE, TRUE &rarr; TRUE. */
    public static boolean isTrue(@Nullable Boolean b) {
        return b != null && b;
    }

    /** NULL &rarr; FALSE, FALSE &rarr; TRUE, TRUE &rarr; FALSE. */
    public static boolean isFalse(@Nullable Boolean b) {
        return b != null && !b;
    }

    /** NULL &rarr; TRUE, FALSE &rarr; TRUE, TRUE &rarr; FALSE. */
    public static boolean isNotTrue(@Nullable Boolean b) {
        return b == null || !b;
    }

    /** NULL &rarr; TRUE, FALSE &rarr; FALSE, TRUE &rarr; TRUE. */
    public static boolean isNotFalse(@Nullable Boolean b) {
        return b == null || b;
    }

    /** NULL &rarr; NULL, FALSE &rarr; TRUE, TRUE &rarr; FALSE. */
    public static Boolean not(Boolean b) {
        return b == null ? castNonNull(null) : !b;
    }

    /** Converts a JDBC array to a list. */
    public static List arrayToList(final java.sql.Array a) {
        if (a == null) {
            return castNonNull(null);
        }
        try {
            return Primitive.asList(a.getArray());
        } catch (SQLException e) {
            throw Util.toUnchecked(e);
        }
    }

    /** Support the {@code CURRENT VALUE OF sequence} operator. */
    @NonDeterministic
    public static long sequenceCurrentValue(String key) {
        return getAtomicLong(key).get();
    }

    /** Support the {@code NEXT VALUE OF sequence} operator. */
    @NonDeterministic
    public static long sequenceNextValue(String key) {
        return getAtomicLong(key).incrementAndGet();
    }

    private static AtomicLong getAtomicLong(String key) {
        final Map<String, AtomicLong> map =
                requireNonNull(THREAD_SEQUENCES.get(), "THREAD_SEQUENCES.get()");
        AtomicLong atomic = map.get(key);
        if (atomic == null) {
            atomic = new AtomicLong();
            map.put(key, atomic);
        }
        return atomic;
    }

    /** Support the ARRAYS_OVERLAP function. */
    public static @Nullable Boolean arraysOverlap(List list1, List list2) {
        if (list1.size() > list2.size()) {
            return arraysOverlap(list2, list1);
        }
        final List smaller = list1;
        final List bigger = list2;
        boolean hasNull = false;
        if (smaller.size() > 0 && bigger.size() > 0) {
            final Set smallestSet = new HashSet(smaller);
            hasNull = smallestSet.remove(null);
            for (Object element : bigger) {
                if (element == null) {
                    hasNull = true;
                } else if (smallestSet.contains(element)) {
                    return true;
                }
            }
        }
        if (hasNull) {
            return null;
        } else {
            return false;
        }
    }

    /** Support the ARRAYS_ZIP function. */
    @SuppressWarnings("argument.type.incompatible")
    public static List arraysZip(List... lists) {
        final int biggestCardinality =
                lists.length == 0 ? 0 : Arrays.stream(lists).mapToInt(List::size).max().getAsInt();

        final List result = new ArrayList(biggestCardinality);
        for (int i = 0; i < biggestCardinality; i++) {
            List<Object> row = new ArrayList<>();
            Object value;
            for (List list : lists) {
                if (i < list.size() && list.get(i) != null) {
                    value = list.get(i);
                } else {
                    value = null;
                }
                row.add(value);
            }
            result.add(row);
        }
        return result;
    }

    /** Support the ARRAY_COMPACT function. */
    public static List compact(List list) {
        final List result = new ArrayList();
        for (Object element : list) {
            if (element != null) {
                result.add(element);
            }
        }
        return result;
    }

    /** Support the ARRAY_APPEND function. */
    public static List arrayAppend(List list, Object element) {
        final List result = new ArrayList(list.size() + 1);
        result.addAll(list);
        result.add(element);
        return result;
    }

    /**
     * Support the ARRAY_DISTINCT function.
     *
     * <p>Note: If the list does not contain null, {@link Util#distinctList(List)} is probably
     * faster.
     */
    public static List distinct(List list) {
        Set result = new LinkedHashSet<>(list);
        return new ArrayList<>(result);
    }

    /** Support the ARRAY_MAX function. */
    public static @Nullable <T extends Object & Comparable<? super T>> T arrayMax(
            List<? extends T> list) {

        T max = null;
        for (int i = 0; i < list.size(); i++) {
            T item = list.get(i);
            if (item != null && (max == null || item.compareTo(max) > 0)) {
                max = item;
            }
        }
        return max;
    }

    /** Support the ARRAY_MIN function. */
    public static @Nullable <T extends Object & Comparable<? super T>> T arrayMin(
            List<? extends T> list) {

        T min = null;
        for (int i = 0; i < list.size(); i++) {
            T item = list.get(i);
            if (item != null && (min == null || item.compareTo(min) < 0)) {
                min = item;
            }
        }
        return min;
    }

    /** Support the ARRAY_PREPEND function. */
    public static List arrayPrepend(List list, Object element) {
        final List result = new ArrayList(list.size() + 1);
        result.add(element);
        result.addAll(list);
        return result;
    }

    /** Support the ARRAY_POSITION function. */
    public static Long arrayPosition(List list, Object element) {
        final int index = list.indexOf(element);
        if (index != -1) {
            return Long.valueOf(index + 1L);
        }
        return 0L;
    }

    /** Support the ARRAY_REMOVE function. */
    public static List arrayRemove(List list, Object element) {
        final List result = new ArrayList();
        for (Object obj : list) {
            if (obj == null || !obj.equals(element)) {
                result.add(obj);
            }
        }
        return result;
    }

    /** Support the ARRAY_REPEAT function. */
    public static @Nullable List<Object> repeat(Object element, Object count) {
        if (count == null) {
            return null;
        }
        int numberOfElement = (int) count;
        if (numberOfElement < 0) {
            numberOfElement = 0;
        }
        return Collections.nCopies(numberOfElement, element);
    }

    /** Support the ARRAY_EXCEPT function. */
    public static List arrayExcept(List list1, List list2) {
        final Set result = new LinkedHashSet<>(list1);
        result.removeAll(list2);
        return new ArrayList<>(result);
    }

    /** Support the ARRAY_INSERT function. */
    public static @Nullable List arrayInsert(List baselist, Object pos, Object val) {
        if (baselist == null || pos == null) {
            return null;
        }
        int posInt = (int) pos;
        Object[] baseArray = baselist.toArray();
        if (posInt == 0 || posInt >= MAX_ARRAY_LENGTH || posInt <= -MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException(
                    "The index 0 is invalid. "
                            + "An index shall be either < 0 or > 0 (the first element has index 1) "
                            + "and not exceeds the allowed limit.");
        }

        boolean usePositivePos = posInt > 0;

        if (usePositivePos) {
            int newArrayLength = Math.max(baseArray.length + 1, posInt);

            if (newArrayLength > MAX_ARRAY_LENGTH) {
                throw new IndexOutOfBoundsException(
                        String.format(
                                Locale.ROOT,
                                "The new array length %s exceeds the allowed limit.",
                                newArrayLength));
            }

            Object[] newArray = new Object[newArrayLength];

            int posIndex = posInt - 1;
            if (posIndex < baseArray.length) {
                System.arraycopy(baseArray, 0, newArray, 0, posIndex);
                newArray[posIndex] = val;
                System.arraycopy(
                        baseArray, posIndex, newArray, posIndex + 1, baseArray.length - posIndex);
            } else {
                System.arraycopy(baseArray, 0, newArray, 0, baseArray.length);
                newArray[posIndex] = val;
            }

            return Arrays.asList(newArray);
        } else {
            int posIndex = posInt;

            boolean newPosExtendsArrayLeft = baseArray.length + posIndex < 0;

            if (newPosExtendsArrayLeft) {
                // special case, if the new position is negative but larger than the current array
                // size
                // place the new item at start of array, place the current array contents at the end
                // and fill the newly created array elements in middle with a null
                int newArrayLength = -posIndex + 1;

                if (newArrayLength > MAX_ARRAY_LENGTH) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    Locale.ROOT,
                                    "The new array length %s exceeds the allowed limit.",
                                    newArrayLength));
                }

                Object[] newArray = new Object[newArrayLength];
                System.arraycopy(
                        baseArray,
                        0,
                        newArray,
                        Math.abs(posIndex + baseArray.length) + 1,
                        baseArray.length);
                newArray[0] = val;

                return Arrays.asList(newArray);
            } else {
                posIndex = posIndex + baseArray.length;

                int newArrayLength = Math.max(baseArray.length + 1, posIndex + 1);

                if (newArrayLength > MAX_ARRAY_LENGTH) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    Locale.ROOT,
                                    "The new array length %s exceeds the allowed limit.",
                                    newArrayLength));
                }

                Object[] newArray = new Object[newArrayLength];

                if (posIndex < baseArray.length) {
                    System.arraycopy(baseArray, 0, newArray, 0, posIndex);
                    newArray[posIndex] = val;
                    System.arraycopy(
                            baseArray,
                            posIndex,
                            newArray,
                            posIndex + 1,
                            baseArray.length - posIndex);
                } else {
                    System.arraycopy(baseArray, 0, newArray, 0, baseArray.length);
                    newArray[posIndex] = val;
                }

                return Arrays.asList(newArray);
            }
        }
    }

    /** Support the ARRAY_INTERSECT function. */
    public static List arrayIntersect(List list1, List list2) {
        final Set result = new LinkedHashSet<>(list1);
        result.retainAll(list2);
        return new ArrayList<>(result);
    }

    /** Support the ARRAY_UNION function. */
    public static List arrayUnion(List list1, List list2) {
        final Set result = new LinkedHashSet<>();
        result.addAll(list1);
        result.addAll(list2);
        return new ArrayList<>(result);
    }

    /** Support the SORT_ARRAY function. */
    public static List sortArray(List list, boolean ascending) {
        Comparator comparator =
                ascending
                        ? Comparator.nullsFirst(Comparator.naturalOrder())
                        : Comparator.nullsLast(Comparator.reverseOrder());
        list.sort(comparator);
        return list;
    }

    /** Support the MAP_CONCAT function. */
    public static Map mapConcat(Map... maps) {
        final Map result = new LinkedHashMap();
        Arrays.stream(maps).forEach(result::putAll);
        return result;
    }

    /** Support the MAP_ENTRIES function. */
    public static List mapEntries(Map<Object, Object> map) {
        final List result = new ArrayList(map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            result.add(Arrays.asList(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /** Support the MAP_KEYS function. */
    public static List mapKeys(Map map) {
        return new ArrayList<>(map.keySet());
    }

    /** Support the MAP_VALUES function. */
    public static List mapValues(Map map) {
        return new ArrayList<>(map.values());
    }

    /** Support the MAP_FROM_ARRAYS function. */
    public static Map mapFromArrays(List keysArray, List valuesArray) {
        if (keysArray.size() != valuesArray.size()) {
            throw RESOURCE.illegalArgumentsInMapFromArraysFunc(keysArray.size(), valuesArray.size())
                    .ex();
        }
        final Map map = new LinkedHashMap<>();
        for (int i = 0; i < keysArray.size(); i++) {
            map.put(keysArray.get(i), valuesArray.get(i));
        }
        return map;
    }

    /** Support the MAP_FROM_ENTRIES function. */
    public static @Nullable Map mapFromEntries(List entries) {
        final Map map = new LinkedHashMap<>();
        for (Object entry : entries) {
            if (entry == null) {
                return null;
            }
            map.put(structAccess(entry, 0, null), structAccess(entry, 1, null));
        }
        return map;
    }

    /**
     * Support the MAP function.
     *
     * <p>odd-indexed elements are keys and even-indexed elements are values.
     */
    public static Map map(Object... args) {
        final Map map = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            Object key = args[i];
            Object value = args[i + 1];
            map.put(key, value);
        }
        return map;
    }

    /** Support the STR_TO_MAP function. */
    public static Map strToMap(String string, String stringDelimiter, String keyValueDelimiter) {
        final Map map = new LinkedHashMap();
        final String[] keyValues = string.split(stringDelimiter, -1);
        for (String s : keyValues) {
            String[] keyValueArray = s.split(keyValueDelimiter, 2);
            String key = keyValueArray[0];
            String value = keyValueArray.length < 2 ? null : keyValueArray[1];
            map.put(key, value);
        }
        return map;
    }

    /** Support the SLICE function. */
    public static List slice(List list) {
        List result = new ArrayList(list.size());
        for (Object e : list) {
            result.add(structAccess(e, 0, null));
        }
        return result;
    }

    /** Support the ELEMENT function. */
    public static @Nullable Object element(List list) {
        switch (list.size()) {
            case 0:
                return null;
            case 1:
                return list.get(0);
            default:
                throw RESOURCE.moreThanOneValueInList(list.toString()).ex();
        }
    }

    /** Support the MEMBER OF function. */
    public static boolean memberOf(@Nullable Object object, Collection collection) {
        return collection.contains(object);
    }

    /** Support the MULTISET INTERSECT DISTINCT function. */
    public static <E> Collection<E> multisetIntersectDistinct(Collection<E> c1, Collection<E> c2) {
        final Set<E> result = new HashSet<>(c1);
        result.retainAll(c2);
        return new ArrayList<>(result);
    }

    /** Support the MULTISET INTERSECT ALL function. */
    public static <E> Collection<E> multisetIntersectAll(Collection<E> c1, Collection<E> c2) {
        final List<E> result = new ArrayList<>(c1.size());
        final List<E> c2Copy = new ArrayList<>(c2);
        for (E e : c1) {
            if (c2Copy.remove(e)) {
                result.add(e);
            }
        }
        return result;
    }

    /** Support the MULTISET EXCEPT ALL function. */
    @SuppressWarnings("JdkObsolete")
    public static <E> Collection<E> multisetExceptAll(Collection<E> c1, Collection<E> c2) {
        // TOOD: use Multisets?
        final List<E> result = new LinkedList<>(c1);
        for (E e : c2) {
            result.remove(e);
        }
        return result;
    }

    /** Support the MULTISET EXCEPT DISTINCT function. */
    public static <E> Collection<E> multisetExceptDistinct(Collection<E> c1, Collection<E> c2) {
        final Set<E> result = new HashSet<>(c1);
        result.removeAll(c2);
        return new ArrayList<>(result);
    }

    /** Support the IS A SET function. */
    public static boolean isASet(Collection collection) {
        if (collection instanceof Set) {
            return true;
        }
        // capacity calculation is in the same way like for new HashSet(Collection)
        // however return immediately in case of duplicates
        Set set = new HashSet(Math.max((int) (collection.size() / .75f) + 1, 16));
        for (Object e : collection) {
            if (!set.add(e)) {
                return false;
            }
        }
        return true;
    }

    /** Support the SUBMULTISET OF function. */
    @SuppressWarnings("JdkObsolete")
    public static boolean submultisetOf(Collection possibleSubMultiset, Collection multiset) {
        if (possibleSubMultiset.size() > multiset.size()) {
            return false;
        }
        // TODO: use Multisets?
        Collection multisetLocal = new LinkedList(multiset);
        for (Object e : possibleSubMultiset) {
            if (!multisetLocal.remove(e)) {
                return false;
            }
        }
        return true;
    }

    /** Support the MULTISET UNION function. */
    public static Collection multisetUnionDistinct(Collection collection1, Collection collection2) {
        // capacity calculation is in the same way like for new HashSet(Collection)
        Set resultCollection =
                new HashSet(
                        Math.max((int) ((collection1.size() + collection2.size()) / .75f) + 1, 16));
        resultCollection.addAll(collection1);
        resultCollection.addAll(collection2);
        return new ArrayList(resultCollection);
    }

    /** Support the MULTISET UNION ALL function. */
    public static Collection multisetUnionAll(Collection collection1, Collection collection2) {
        List resultCollection = new ArrayList(collection1.size() + collection2.size());
        resultCollection.addAll(collection1);
        resultCollection.addAll(collection2);
        return resultCollection;
    }

    /** Support the ARRAY_REVERSE function. */
    public static List reverse(List list) {
        Collections.reverse(list);
        return list;
    }

    /** SQL {@code ARRAY_TO_STRING(array, delimiter)} function. */
    public static String arrayToString(List list, String delimiter) {
        return arrayToString(list, delimiter, null);
    }

    /** SQL {@code ARRAY_TO_STRING(array, delimiter, nullText)} function. */
    public static String arrayToString(List list, String delimiter, @Nullable String nullText) {
        // Note that the SQL function ARRAY_TO_STRING that we implement will return
        // 'NULL' when the nullText argument is NULL. However, that is handled by
        // the nullPolicy of the RexToLixTranslator. So here a NULL value
        // for the nullText argument can only come from the above 2-argument version.
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (Object item : list) {
            String str;
            if (item == null) {
                if (nullText == null) {
                    continue;
                } else {
                    str = nullText;
                }
            } else if (item instanceof String) {
                str = (String) item;
            } else if (item instanceof ByteString) {
                str = item.toString();
            } else {
                throw new IllegalStateException(
                        "arrayToString supports only String or ByteString, but got "
                                + item.getClass().getName());
            }

            if (!isFirst) {
                sb.append(delimiter);
            }
            sb.append(str);
            isFirst = false;
        }
        return sb.toString();
    }

    /**
     * Function that, given a certain List containing single-item structs (i.e. arrays / lists with
     * a single item), builds an Enumerable that returns those single items inside the structs.
     */
    public static Function1<List<Object>, Enumerable<Object>> flatList() {
        return inputList -> Linq4j.asEnumerable(inputList).select(v -> structAccess(v, 0, null));
    }

    public static Function1<Object, Enumerable<ComparableList<Comparable>>> flatProduct(
            final int[] fieldCounts,
            final boolean withOrdinality,
            final FlatProductInputType[] inputTypes) {
        if (fieldCounts.length == 1) {
            if (!withOrdinality && inputTypes[0] == FlatProductInputType.SCALAR) {
                //noinspection unchecked
                return (Function1) LIST_AS_ENUMERABLE;
            } else {
                return row -> p2(new Object[] {row}, fieldCounts, withOrdinality, inputTypes);
            }
        }
        return lists -> p2((Object[]) lists, fieldCounts, withOrdinality, inputTypes);
    }

    private static Enumerable<FlatLists.ComparableList<Comparable>> p2(
            Object[] lists,
            int[] fieldCounts,
            boolean withOrdinality,
            FlatProductInputType[] inputTypes) {
        final List<Enumerator<List<Comparable>>> enumerators = new ArrayList<>();
        int totalFieldCount = 0;
        for (int i = 0; i < lists.length; i++) {
            int fieldCount = fieldCounts[i];
            FlatProductInputType inputType = inputTypes[i];
            Object inputObject = lists[i];
            switch (inputType) {
                case SCALAR:
                    @SuppressWarnings("unchecked")
                    List<Comparable> list = (List<Comparable>) inputObject;
                    enumerators.add(Linq4j.transform(Linq4j.enumerator(list), FlatLists::of));
                    break;
                case LIST:
                    @SuppressWarnings("unchecked")
                    List<List<Comparable>> listList = (List<List<Comparable>>) inputObject;
                    enumerators.add(Linq4j.enumerator(listList));
                    break;
                case MAP:
                    @SuppressWarnings("unchecked")
                    Map<Comparable, Comparable> map = (Map<Comparable, Comparable>) inputObject;
                    Enumerator<Map.Entry<Comparable, Comparable>> enumerator =
                            Linq4j.enumerator(map.entrySet());

                    Enumerator<List<Comparable>> transformed =
                            Linq4j.transform(
                                    enumerator, e -> FlatLists.of(e.getKey(), e.getValue()));
                    enumerators.add(transformed);
                    break;
                default:
                    break;
            }
            if (fieldCount < 0) {
                ++totalFieldCount;
            } else {
                totalFieldCount += fieldCount;
            }
        }
        if (withOrdinality) {
            ++totalFieldCount;
        }
        return product(enumerators, totalFieldCount, withOrdinality);
    }

    public static Object[] array(Object... args) {
        return args;
    }

    /**
     * Returns whether there is an element in {@code list} for which {@code predicate} is true.
     * Also, if {@code predicate} returns null for any element of {@code list} and does not return
     * {@code true} for any element of {@code list}, the result will be {@code null}, not {@code
     * false}.
     */
    public static @Nullable <E> Boolean nullableExists(
            List<? extends E> list, Function1<E, Boolean> predicate) {
        boolean nullExists = false;
        for (E e : list) {
            Boolean res = predicate.apply(e);
            if (res == null) {
                nullExists = true;
            } else if (res) {
                return true;
            }
        }
        return nullExists ? null : false;
    }

    /**
     * Returns whether {@code predicate} is true for all elements of {@code list}. Also, if {@code
     * predicate} returns null for any element of {@code list} and does not return {@code false} for
     * any element, the result will be {@code null}, not {@code true}.
     */
    public static @Nullable <E> Boolean nullableAll(
            List<? extends E> list, Function1<E, Boolean> predicate) {
        boolean nullExists = false;
        for (E e : list) {
            Boolean res = predicate.apply(e);
            if (res == null) {
                nullExists = true;
            } else if (!res) {
                return false;
            }
        }
        return nullExists ? null : true;
    }

    /**
     * Similar to {@link Linq4j#product(Iterable)} but each resulting list implements {@link
     * FlatLists.ComparableList}.
     */
    public static <E extends Comparable> Enumerable<FlatLists.ComparableList<E>> product(
            final List<Enumerator<List<E>>> enumerators,
            final int fieldCount,
            final boolean withOrdinality) {
        return new AbstractEnumerable<FlatLists.ComparableList<E>>() {
            @Override
            public Enumerator<FlatLists.ComparableList<E>> enumerator() {
                return new ProductComparableListEnumerator<>(
                        enumerators, fieldCount, withOrdinality);
            }
        };
    }

    /**
     * Implements the {@code .} (field access) operator on an object whose type is not known until
     * runtime.
     *
     * <p>A struct object can be represented in various ways by the runtime and depends on the
     * {@link org.apache.calcite.adapter.enumerable.JavaRowFormat}.
     */
    @Experimental
    public static @Nullable Object structAccess(
            @Nullable Object structObject, int index, @Nullable String fieldName) {
        if (structObject == null) {
            return null;
        }

        if (structObject instanceof Object[]) {
            return ((Object[]) structObject)[index];
        } else if (structObject instanceof List) {
            return ((List) structObject).get(index);
        } else if (structObject instanceof Row) {
            return ((Row) structObject).getObject(index);
        } else {
            Class<?> beanClass = structObject.getClass();
            try {
                if (fieldName == null) {
                    throw new IllegalStateException(
                            "Field name cannot be null for struct field access");
                }
                Field structField = beanClass.getDeclaredField(fieldName);
                return structField.get(structObject);
            } catch (NoSuchFieldException | IllegalAccessException ex) {
                throw RESOURCE.failedToAccessField(fieldName, index, beanClass.getName()).ex(ex);
            }
        }
    }

    /**
     * Enumerates over the cartesian product of the given lists, returning a comparable list for
     * each row.
     *
     * @param <E> element type
     */
    private static class ProductComparableListEnumerator<E extends Comparable>
            extends CartesianProductEnumerator<List<E>, FlatLists.ComparableList<E>> {
        final Object[] flatElements;
        final List<Object> list;
        private final boolean withOrdinality;
        private int ordinality;

        ProductComparableListEnumerator(
                List<Enumerator<List<E>>> enumerators, int fieldCount, boolean withOrdinality) {
            super(enumerators);
            this.withOrdinality = withOrdinality;
            flatElements = new Object[fieldCount];
            list = Arrays.asList(flatElements);
        }

        @Override
        public boolean moveNext() {
            boolean hasNext = super.moveNext();
            if (hasNext && withOrdinality) {
                ordinality++;
            }
            return hasNext;
        }

        @Override
        public FlatLists.ComparableList<E> current() {
            int i = 0;
            for (Object element : (Object[]) elements) {
                Object[] a;
                if (element.getClass().isArray()) {
                    a = (Object[]) element;
                } else {
                    final List list2 = (List) element;
                    a = list2.toArray();
                }
                System.arraycopy(a, 0, flatElements, i, a.length);
                i += a.length;
            }
            if (withOrdinality) {
                flatElements[i] = ordinality;
            }
            //noinspection unchecked
            return (FlatLists.ComparableList) FlatLists.of(list);
        }

        @Override
        public void reset() {
            super.reset();
            if (withOrdinality) {
                ordinality = 0;
            }
        }
    }

    /** Specifies scope to search for {@code #containsSubstr}. */
    public enum JsonScope {
        JSON_KEYS,
        JSON_KEYS_AND_VALUES,
        JSON_VALUES
    }

    /** Type of argument passed into {@link #flatProduct}. */
    public enum FlatProductInputType {
        SCALAR,
        LIST,
        MAP
    }

    /** Type of part to extract passed into {@link ParseUrlFunction#parseUrl}. */
    private enum PartToExtract {
        HOST,
        PATH,
        QUERY,
        REF,
        PROTOCOL,
        FILE,
        AUTHORITY,
        USERINFO;
    }
}
