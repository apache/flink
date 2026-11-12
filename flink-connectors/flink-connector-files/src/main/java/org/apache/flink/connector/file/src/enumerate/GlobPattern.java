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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** A filesystem-independent, segment-wise glob and its fixed search root. */
final class GlobPattern {

    private final Path root;
    private final List<Segment> segments = new ArrayList<>();

    GlobPattern(Path path) {
        final Path normalized =
                path.getPath().isEmpty()
                        ? path
                        : new Path(
                                path.toUri().getScheme(),
                                path.toUri().getAuthority(),
                                path.getPath());
        final String[] parts = normalized.getPath().split("/");
        boolean glob = false;
        Path prefix = normalized;
        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }
            glob |= part.indexOf('*') >= 0 || part.indexOf('?') >= 0 || part.indexOf('[') >= 0;
            if (glob) {
                segments.add(new Segment(part));
                prefix = prefix.getParent();
            }
        }
        root = prefix;
    }

    Path getRoot() {
        return root;
    }

    boolean hasGlob() {
        return !segments.isEmpty();
    }

    Set<Integer> initialPositions() {
        final Set<Integer> positions = new HashSet<>();
        addPosition(positions, 0);
        return positions;
    }

    boolean isComplete(Set<Integer> positions) {
        return positions.contains(segments.size());
    }

    Set<Integer> matchChild(FileStatus child, Set<Integer> positions) {
        final Set<Integer> next = new HashSet<>();
        for (int position : positions) {
            if (position == segments.size()) {
                continue;
            }
            final Segment segment = segments.get(position);
            if (segment.recursive) {
                if (child.isDir()) {
                    addPosition(next, position);
                }
            } else if (segment.matches(child.getPath().getName())) {
                if (child.isDir()) {
                    addPosition(next, position + 1);
                } else if (position + 1 == segments.size()) {
                    next.add(position + 1);
                }
            }
        }
        return next;
    }

    private void addPosition(Set<Integer> positions, int position) {
        positions.add(position);
        // A recursive segment also accepts zero directories, including consecutive ** segments.
        while (position < segments.size() && segments.get(position).recursive) {
            positions.add(++position);
        }
    }

    private static final class Segment {

        private static final IntPredicate STAR = codePoint -> true;
        private static final IntPredicate ANY_CHARACTER = codePoint -> true;

        private final boolean recursive;
        private final List<IntPredicate> tokens = new ArrayList<>();

        private Segment(String glob) {
            recursive = glob.equals("**");
            for (int i = 0; i < glob.length(); i++) {
                final char c = glob.charAt(i);
                if (c == '*') {
                    tokens.add(STAR);
                    while (i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                        i++;
                    }
                } else if (c == '?') {
                    tokens.add(ANY_CHARACTER);
                } else if (c == '[') {
                    final StringBuilder regex = new StringBuilder();
                    i = appendCharacterClass(glob, i, regex);
                    final Pattern characterClass =
                            Pattern.compile(regex.toString(), Pattern.DOTALL);
                    tokens.add(
                            codePoint ->
                                    characterClass
                                            .matcher(new String(Character.toChars(codePoint)))
                                            .matches());
                } else {
                    final int literal = glob.codePointAt(i);
                    tokens.add(codePoint -> codePoint == literal);
                    i += Character.charCount(literal) - 1;
                }
            }
        }

        private boolean matches(String name) {
            final int[] codePoints = name.codePoints().toArray();
            boolean[] previous = new boolean[codePoints.length + 1];
            previous[0] = true;
            // Dynamic programming bounds matching by pattern length times filename length.
            // Backtracking regexes for repeated stars can otherwise stall file discovery.
            for (IntPredicate token : tokens) {
                final boolean[] next = new boolean[codePoints.length + 1];
                next[0] = token == STAR && previous[0];
                for (int i = 1; i <= codePoints.length; i++) {
                    next[i] =
                            token == STAR
                                    ? previous[i] || next[i - 1]
                                    : previous[i - 1] && token.test(codePoints[i - 1]);
                }
                previous = next;
            }
            return previous[codePoints.length];
        }

        private static int appendCharacterClass(String glob, int start, StringBuilder regex) {
            int first = start + 1;
            final boolean negate = first < glob.length() && glob.charAt(first) == '!';
            if (negate) {
                first++;
            }
            // A closing bracket in the first position is a member, as in []] or [!]].
            int end = first;
            if (end < glob.length() && glob.charAt(end) == ']') {
                end++;
            }
            while (end < glob.length() && glob.charAt(end) != ']') {
                end++;
            }
            if (end == glob.length() || end == first) {
                throw new PatternSyntaxException("Unclosed or empty character class", glob, start);
            }
            regex.append(negate ? "[^" : "[");
            for (int i = first; i < end; ) {
                final int codePoint = glob.codePointAt(i);
                final int following = i + Character.charCount(codePoint);
                if (codePoint == '-' && i > first && following < end) {
                    regex.append('-');
                } else {
                    // Quoting also prevents Java regex intersection/nested-class syntax.
                    regex.append("\\x{").append(Integer.toHexString(codePoint)).append('}');
                }
                i = following;
            }
            regex.append(']');
            return end;
        }
    }
}
