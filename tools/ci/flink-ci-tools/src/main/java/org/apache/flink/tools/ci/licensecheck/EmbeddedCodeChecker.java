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

package org.apache.flink.tools.ci.licensecheck;

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Checks that a jar ships the licences and NOTICE attribution for third-party code that a bundled
 * dependency embeds in its own classes or native libraries, such as the code Netty contains.
 *
 * <p>The components, their markers and what they require are listed in {@code embedded-code.rules}.
 */
final class EmbeddedCodeChecker {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedCodeChecker.class);

    private static final String LICENSES_DIRECTORY = "META-INF/licenses/";

    // AIX libraries are archives, as in snappy-java's org/xerial/snappy/native/AIX/
    private static final Pattern NATIVE_LIBRARY =
            Pattern.compile(
                    "(?i)(.*/)?[^/]+\\.(so(\\.\\d+)*|dylib|jnilib|dll)|(.*/)?native/.+\\.a");

    // grpc-netty-shaded renames it to META-INF/io.grpc.netty.shaded.io.netty.versions.properties
    private static final Pattern NETTY_VERSIONS_FILE =
            Pattern.compile("(.*/)?META-INF/([\\w.]+\\.)?io\\.netty\\.versions\\.properties");
    private static final Pattern NETTY_VERSION_ENTRY =
            Pattern.compile("(netty-[\\w-]+)\\.version=(\\d+)\\.(\\d+)\\.(\\d+)\\S*");

    private static final Rules RULES = Rules.load("embedded-code.rules");

    private EmbeddedCodeChecker() {}

    static int checkJar(Path jar, Path jarRoot) throws IOException {
        final List<String> entries;
        try (Stream<Path> files = Files.walk(jarRoot)) {
            entries =
                    files.filter(path -> !Files.isDirectory(path))
                            .map(path -> jarRoot.relativize(path).toString())
                            .collect(Collectors.toList());
        }
        return check(RULES, jar, jarRoot, entries);
    }

    /**
     * Lists the entries of the jars nested in this jar, such as the flink-rpc-akka.jar inside
     * flink-rpc-akka-loader, with the contents of those that a rule inspects. The outer jar ships
     * the licences of their code.
     */
    private static Map<String, byte[]> nestedEntries(
            Rules rules, Path jarRoot, List<String> entries) throws IOException {
        final Map<String, byte[]> nested = new LinkedHashMap<>();
        for (String entry : entries) {
            if (!entry.endsWith(".jar")) {
                continue;
            }
            try (ZipInputStream in =
                    new ZipInputStream(Files.newInputStream(jarRoot.resolve(entry)))) {
                for (ZipEntry nestedEntry = in.getNextEntry();
                        nestedEntry != null;
                        nestedEntry = in.getNextEntry()) {
                    final String name = nestedEntry.getName();
                    final boolean inspected =
                            rules.rules.stream()
                                    .anyMatch(
                                            r -> r.markerContent != null && r.matchesMarker(name));
                    nested.put(name, inspected ? readAll(in) : null);
                }
            }
        }
        return nested;
    }

    private static byte[] readAll(InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final byte[] buffer = new byte[8192];
        for (int read = in.read(buffer); read >= 0; read = in.read(buffer)) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    @VisibleForTesting
    static int check(Rules rules, Path jar, Path jarRoot, List<String> entries) throws IOException {
        final Set<String> justifiedLicenses = new HashSet<>();
        final Map<Rule, String> matchedRules = new LinkedHashMap<>();
        final List<String> unknownNatives = new ArrayList<>();
        final Set<String> licenseFiles = new HashSet<>();
        final List<String> versionFiles = new ArrayList<>();
        for (String entry : entries) {
            if (entry.startsWith(LICENSES_DIRECTORY)) {
                licenseFiles.add(entry.substring(LICENSES_DIRECTORY.length()));
            }
            if (NETTY_VERSIONS_FILE.matcher(entry).matches()) {
                versionFiles.add(entry);
            }
            matchEntry(
                    rules,
                    entry,
                    () -> Files.readAllBytes(jarRoot.resolve(entry)),
                    matchedRules,
                    unknownNatives);
        }
        for (Map.Entry<String, byte[]> nested : nestedEntries(rules, jarRoot, entries).entrySet()) {
            matchEntry(rules, nested.getKey(), nested::getValue, matchedRules, unknownNatives);
        }

        final String notice = readNormalized(jarRoot.resolve("META-INF/NOTICE")).orElse("");

        int issues = 0;
        for (Map.Entry<Rule, String> matched : matchedRules.entrySet()) {
            final Rule rule = matched.getKey();
            final String example = matched.getValue();
            justifiedLicenses.addAll(rule.licenses);

            if (!rule.licenses.isEmpty()) {
                final List<String> present =
                        rule.licenses.stream()
                                .filter(licenseFiles::contains)
                                .collect(Collectors.toList());
                if (present.isEmpty()) {
                    LOG.error(
                            "Jar {} contains {} ({}) but none of {} in {}.",
                            jar,
                            rule.name,
                            example,
                            rule.licenses,
                            LICENSES_DIRECTORY);
                    issues++;
                } else if (rule.licenseText != null
                        && !containsLicenseText(jarRoot, present, rule.licenseText)) {
                    LOG.error(
                            "Jar {} contains {} ({}) but {} in {} doesn't contain '{}'.",
                            jar,
                            rule.name,
                            example,
                            present,
                            LICENSES_DIRECTORY,
                            rule.licenseText);
                    issues++;
                }
            }

            for (String noticeText : rule.noticeTexts) {
                if (!notice.contains(normalize(noticeText))) {
                    LOG.error(
                            "Jar {} contains {} ({}) but its META-INF/NOTICE doesn't mention '{}'.",
                            jar,
                            rule.name,
                            example,
                            noticeText);
                    issues++;
                }
            }
        }

        for (String license : new TreeSet<>(rules.allLicenses)) {
            if (licenseFiles.contains(license) && !justifiedLicenses.contains(license)) {
                LOG.error(
                        "Jar {} ships {}{} but none of the code it covers.",
                        jar,
                        LICENSES_DIRECTORY,
                        license);
                issues++;
            }
        }

        for (String unknownNative : unknownNatives) {
            LOG.error(
                    "Jar {} contains the native library {}, which embedded-code.rules doesn't know. Find the third-party code it links and add a rule, or list it as the project's own code.",
                    jar,
                    unknownNative);
            issues++;
        }

        for (String versionFile : versionFiles) {
            issues += checkNettyVersions(rules, jar, jarRoot.resolve(versionFile));
        }

        return issues;
    }

    /** Content of an entry, read only when a rule's marker-content needs it. */
    private interface Content {
        byte[] get() throws IOException;
    }

    private static void matchEntry(
            Rules rules,
            String entry,
            Content content,
            Map<Rule, String> matchedRules,
            List<String> unknownNatives)
            throws IOException {
        boolean known = false;
        for (Rule rule : rules.rules) {
            if (rule.matchesMarker(entry)
                    && (rule.markerContent == null
                            || rule.markerContent
                                    .matcher(new String(content.get(), StandardCharsets.UTF_8))
                                    .find())) {
                matchedRules.putIfAbsent(rule, entry);
                known = true;
            }
        }
        if (!known
                && NATIVE_LIBRARY.matcher(entry).matches()
                && rules.nativePatterns.stream().noneMatch(p -> p.matcher(entry).matches())) {
            unknownNatives.add(entry);
        }
    }

    private static int checkNettyVersions(Rules rules, Path jar, Path versionFile)
            throws IOException {
        final Set<String> unverified = new TreeSet<>();
        for (String line : Files.readAllLines(versionFile, StandardCharsets.UTF_8)) {
            final Matcher matcher = NETTY_VERSION_ENTRY.matcher(line.trim());
            if (!matcher.matches() || matcher.group(1).startsWith("netty-tcnative")) {
                continue;
            }
            final int[] version = {
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)),
                Integer.parseInt(matcher.group(4))
            };
            if (!rules.isVerified(version)) {
                unverified.add(version[0] + "." + version[1] + "." + version[2]);
            }
        }
        for (String version : unverified) {
            LOG.error(
                    "Jar {} bundles Netty {}, outside the versions embedded-code.rules was checked against (up to {}).",
                    jar,
                    version,
                    rules.verifiedVersionsDescription());
        }
        return unverified.size();
    }

    private static boolean containsLicenseText(Path jarRoot, List<String> files, String text)
            throws IOException {
        for (String file : files) {
            final Optional<String> contents =
                    readNormalized(jarRoot.resolve(LICENSES_DIRECTORY + file));
            if (contents.isPresent() && contents.get().contains(normalize(text))) {
                return true;
            }
        }
        return false;
    }

    private static Optional<String> readNormalized(Path file) throws IOException {
        return read(file).map(EmbeddedCodeChecker::normalize);
    }

    private static Optional<String> read(Path file) throws IOException {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        return Optional.of(new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
    }

    private static String normalize(String text) {
        return text.replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
    }

    /** One embedded component: where its code shows up and what a jar containing it owes. */
    @VisibleForTesting
    static final class Rule {
        final String name;
        final List<Pattern> markers = new ArrayList<>();
        final List<String> licenses = new ArrayList<>();
        final List<String> noticeTexts = new ArrayList<>();
        String licenseText;
        Pattern markerContent;

        Rule(String name) {
            this.name = name;
        }

        boolean matchesMarker(String entry) {
            for (Pattern marker : markers) {
                if (marker.matcher(entry).matches()) {
                    return true;
                }
            }
            return false;
        }
    }

    /** The parsed contents of a rules file. */
    @VisibleForTesting
    static final class Rules {
        final List<Rule> rules = new ArrayList<>();
        final List<Pattern> nativePatterns = new ArrayList<>();
        final Set<String> allLicenses = new HashSet<>();
        final Map<String, Integer> verifiedPatchByMinor = new HashMap<>();

        boolean isVerified(int[] version) {
            final Integer verifiedPatch = verifiedPatchByMinor.get(version[0] + "." + version[1]);
            return verifiedPatch != null && version[2] <= verifiedPatch;
        }

        String verifiedVersionsDescription() {
            return verifiedPatchByMinor.entrySet().stream()
                    .map(e -> e.getKey() + "." + e.getValue())
                    .sorted()
                    .collect(Collectors.joining(", "));
        }

        static Rules load(String resource) {
            try (InputStream in =
                    Objects.requireNonNull(
                            EmbeddedCodeChecker.class.getResourceAsStream("/" + resource),
                            resource)) {
                return parse(
                        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                                .lines()
                                .collect(Collectors.toList()));
            } catch (IOException e) {
                // called from a static initializer
                throw new RuntimeException("Could not load " + resource, e);
            }
        }

        @VisibleForTesting
        static Rules parse(List<String> lines) {
            final Rules rules = new Rules();
            Rule current = null;
            for (String rawLine : lines) {
                final String line = rawLine.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    current = new Rule(line.substring(1, line.length() - 1));
                    rules.rules.add(current);
                    continue;
                }
                final int separator = line.indexOf('=');
                if (separator < 0) {
                    throw new IllegalArgumentException("Malformed rule line: " + rawLine);
                }
                final String key = line.substring(0, separator).trim();
                final String value = line.substring(separator + 1).trim();
                if (key.equals("verified-netty")) {
                    final Matcher matcher =
                            Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\.\\w+").matcher(value);
                    if (!matcher.matches()) {
                        throw new IllegalArgumentException("Malformed Netty version: " + value);
                    }
                    rules.verifiedPatchByMinor.put(
                            matcher.group(1) + "." + matcher.group(2),
                            Integer.parseInt(matcher.group(3)));
                    continue;
                }
                if (current == null) {
                    throw new IllegalArgumentException("Key outside of a section: " + rawLine);
                }
                switch (key) {
                    case "marker":
                        current.markers.add(Pattern.compile(value));
                        break;
                    case "license":
                        current.licenses.add(value);
                        rules.allLicenses.add(value);
                        break;
                    case "marker-content":
                        current.markerContent = Pattern.compile(value);
                        break;
                    case "license-text":
                        current.licenseText = value;
                        break;
                    case "notice":
                        current.noticeTexts.add(value);
                        break;
                    case "native":
                        rules.nativePatterns.add(Pattern.compile(value));
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown key: " + rawLine);
                }
            }
            rules.rules.removeIf(rule -> rule.markers.isEmpty());
            return rules;
        }
    }
}
