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

package org.apache.flink.table.client.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;

import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.StyleResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Prompt handler class which allows customization for the prompt shown at the start (left prompt)
 * and the end (right prompt) of each line.
 */
public class PromptHandler {
    private static final char ESCAPE_BACKSLASH = '\\';
    private static final Map<String, SimpleDateFormat> FORMATTER_CACHE = new HashMap<>();

    static {
        FORMATTER_CACHE.put("D", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT));
        FORMATTER_CACHE.put("m", new SimpleDateFormat("mm", Locale.ROOT));
        FORMATTER_CACHE.put("o", new SimpleDateFormat("MM", Locale.ROOT));
        FORMATTER_CACHE.put("O", new SimpleDateFormat("MMM", Locale.ROOT));
        FORMATTER_CACHE.put("P", new SimpleDateFormat("aa", Locale.ROOT));
        FORMATTER_CACHE.put("r", new SimpleDateFormat("hh:mm", Locale.ROOT));
        FORMATTER_CACHE.put("R", new SimpleDateFormat("HH:mm", Locale.ROOT));
        FORMATTER_CACHE.put("s", new SimpleDateFormat("ss", Locale.ROOT));
        FORMATTER_CACHE.put("w", new SimpleDateFormat("d", Locale.ROOT));
        FORMATTER_CACHE.put("W", new SimpleDateFormat("E", Locale.ROOT));
        FORMATTER_CACHE.put("y", new SimpleDateFormat("yy", Locale.ROOT));
        FORMATTER_CACHE.put("Y", new SimpleDateFormat("yyyy", Locale.ROOT));
    }

    private static final StyleResolver STYLE_RESOLVER = new StyleResolver(s -> "");

    private final Executor executor;
    private final Supplier<Terminal> terminalSupplier;

    public PromptHandler(Executor executor, Supplier<Terminal> terminalSupplier) {
        this.executor = executor;
        this.terminalSupplier = terminalSupplier;
    }

    public String getPrompt() {
        return buildPrompt(
                executor.getSessionConfig().get(SqlClientOptions.PROMPT),
                SqlClientOptions.PROMPT.defaultValue());
    }

    public String getRightPrompt() {
        return buildPrompt(
                executor.getSessionConfig().get(SqlClientOptions.RIGHT_PROMPT),
                SqlClientOptions.RIGHT_PROMPT.defaultValue());
    }

    @Nonnull
    private String buildPrompt(@Nullable String pattern, String defaultValue) {
        if (pattern == null) {
            return defaultValue;
        }
        AttributedStringBuilder promptStringBuilder = new AttributedStringBuilder();
        try {
            String currentCatalog = null;
            String currentDatabase = null;
            for (int i = 0; i < pattern.length(); i++) {
                final char c = pattern.charAt(i);
                if (c == ESCAPE_BACKSLASH) {
                    if (i == pattern.length() - 1) {
                        continue;
                    }
                    final char nextChar = pattern.charAt(i + 1);
                    switch (nextChar) {
                        case 'D':
                        case 'm':
                        case 'o':
                        case 'O':
                        case 'P':
                        case 'r':
                        case 'R':
                        case 's':
                        case 'w':
                        case 'W':
                        case 'y':
                        case 'Y':
                            promptStringBuilder.append(
                                    FORMATTER_CACHE
                                            .get(String.valueOf(nextChar))
                                            .format(new Date()));
                            i++;
                            break;
                        case 'c':
                            if (currentCatalog == null) {
                                currentCatalog = executor.getCurrentCatalog();
                            }
                            promptStringBuilder.append(currentCatalog);
                            i++;
                            break;
                        case 'd':
                            if (currentDatabase == null) {
                                currentDatabase = executor.getCurrentDatabase();
                            }
                            promptStringBuilder.append(currentDatabase);
                            i++;
                            break;
                        case ESCAPE_BACKSLASH:
                            promptStringBuilder.append(ESCAPE_BACKSLASH);
                            i++;
                            break;
                            // date time pattern \{...\}
                        case '{':
                            int dateTimeMaskCloseIndex =
                                    pattern.indexOf(ESCAPE_BACKSLASH + "}", i + 1);
                            if (dateTimeMaskCloseIndex > 0) {
                                String mask = pattern.substring(i + 2, dateTimeMaskCloseIndex);
                                FORMATTER_CACHE.computeIfAbsent(
                                        mask, v -> new SimpleDateFormat(mask, Locale.ROOT));
                                promptStringBuilder.append(
                                        FORMATTER_CACHE.get(mask).format(new Date()));
                                i = dateTimeMaskCloseIndex + 1;
                            }
                            break;
                            // color and style pattern \[...\]
                        case '[':
                            int closeBracketIndex = pattern.indexOf(ESCAPE_BACKSLASH + "]", i + 1);
                            if (closeBracketIndex > 0) {
                                String color = pattern.substring(i + 2, closeBracketIndex);
                                AttributedStyle style = STYLE_RESOLVER.resolve(color);
                                promptStringBuilder.style(style);
                                i = closeBracketIndex + 1;
                            }
                            break;
                            // property value pattern \:...\:
                        case ':':
                            int nextColonIndex = pattern.indexOf(ESCAPE_BACKSLASH + ":", i + 1);
                            String propertyValue;
                            if (nextColonIndex > 0) {
                                String propertyName = pattern.substring(i + 2, nextColonIndex);
                                propertyValue =
                                        ((Configuration) executor.getSessionConfig())
                                                .toMap()
                                                .get(propertyName);
                                promptStringBuilder.append(propertyValue);
                                i = nextColonIndex + 1;
                            }
                            break;
                    }
                } else {
                    promptStringBuilder.append(c);
                }
            }
            return promptStringBuilder.toAnsi();
        } catch (Exception e) {
            final boolean isVerbose = executor.getSessionConfig().get(SqlClientOptions.VERBOSE);
            try (PrintWriter writer = terminalSupplier.get().writer()) {
                writer.println(CliStrings.messageError(e.getMessage(), e, isVerbose).toAnsi());
            }
            return defaultValue;
        }
    }
}
