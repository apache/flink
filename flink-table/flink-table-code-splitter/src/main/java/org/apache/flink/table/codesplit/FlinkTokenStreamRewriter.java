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

package org.apache.flink.table.codesplit;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Replacement for {@link org.antlr.v4.runtime.TokenStreamRewriter} with the same output for the
 * operations used by the code splitter. {@code TokenStreamRewriter#getText} is quadratic in the
 * number of edits, here edits are applied when recorded and {@link #getText} is a single pass.
 *
 * <p>As in {@code TokenStreamRewriter}: inserts at the start of a replace recorded before it become
 * part of its text, a replace drops earlier replaces it contains and earlier inserts strictly
 * inside it, overlapping deletes are merged, other overlaps throw, inserts into a replaced range
 * fail {@link #getText} unless a later replace drops it, inserts at the start of a replace are
 * emitted before it in reverse order.
 *
 * <p>Unlike {@code TokenStreamRewriter}, {@link #getText} does not change the recorded edits, so
 * repeated calls match its first call, and nothing is printed to stdout on merged deletes.
 */
final class FlinkTokenStreamRewriter {

    private final CommonTokenStream tokens;
    private final int size;
    // replace starting at a token: its last token index (-1 if none) and text (null for delete)
    private final int[] replaceStop;
    private final String[] replaceText;
    // start of the replace covering a token, -1 if none
    private final int[] coveredBy;
    // inserts before a token in instruction order, the last slot is after EOF
    private final List<List<Insert>> inserts;
    // starts of replaces with an insert inside them, TokenStreamRewriter fails on these in getText
    private final Set<Integer> replacesWithInserts = new HashSet<>();
    // starts of merged deletes with inserts TokenStreamRewriter keeps apart, it fails on these in
    // getText unless a later insert there combines with them
    private final Set<Integer> orphanInserts = new HashSet<>();

    FlinkTokenStreamRewriter(CommonTokenStream tokens) {
        tokens.fill();
        this.tokens = tokens;
        this.size = tokens.size();
        this.replaceStop = new int[size];
        this.replaceText = new String[size];
        this.coveredBy = new int[size];
        this.inserts = new ArrayList<>(Collections.nCopies(size + 1, null));
        Arrays.fill(replaceStop, -1);
        Arrays.fill(coveredBy, -1);
    }

    void insertBefore(Token token, String text) {
        insert(token.getTokenIndex(), text, false);
    }

    void insertAfter(Token token, String text) {
        insert(token.getTokenIndex() + 1, text, true);
    }

    void replace(Token token, String text) {
        replace(token, token, text);
    }

    void replace(Token start, Token stop, String text) {
        replace(start.getTokenIndex(), stop.getTokenIndex(), text);
    }

    void delete(Token start, Token stop) {
        replace(start.getTokenIndex(), stop.getTokenIndex(), null);
    }

    String getText() {
        return getText(0, size - 1);
    }

    String getText(Interval interval) {
        return getText(interval.a, interval.b);
    }

    private void insert(int index, String text, boolean after) {
        if (index < size && coveredBy[index] != -1 && coveredBy[index] != index) {
            // fails getText unless a later replace drops it, kept for a replace starting here
            replacesWithInserts.add(coveredBy[index]);
        }
        if (inserts.get(index) == null) {
            inserts.set(index, new ArrayList<>(1));
        }
        inserts.get(index).add(new Insert(text, after));
        if (orphanInserts.remove(index)) {
            Insert combined = new Insert(combine(inserts.get(index)), after);
            inserts.set(index, new ArrayList<>(Collections.singletonList(combined)));
        }
    }

    private void replace(int start, int stop, String text) {
        // as in TokenStreamRewriter, earlier inserts at start become part of the replace text
        if (inserts.get(start) != null) {
            StringBuilder sb = new StringBuilder();
            appendInsertsReversed(sb, start);
            text = text == null ? sb.toString() : sb.append(text).toString();
            inserts.set(start, null);
            orphanInserts.remove(start);
        }
        // only inserts inside this range are dropped, not inside a delete it merges with
        for (int i = start + 1; i <= stop; i++) {
            inserts.set(i, null);
            if (!orphanInserts.isEmpty()) {
                orphanInserts.remove(i);
            }
        }
        record(start, stop, text);
    }

    private void record(int start, int stop, String text) {
        for (int i = start; i <= stop; i++) {
            int prevStart = coveredBy[i];
            if (prevStart == -1) {
                continue;
            }
            int prevStop = replaceStop[prevStart];
            boolean bothDeletes = text == null && replaceText[prevStart] == null;
            if (prevStart >= start && prevStop <= stop) {
                dropReplace(prevStart);
            } else if (bothDeletes) {
                dropReplace(prevStart);
                if (prevStart < start && inserts.get(prevStart) != null) {
                    orphanInserts.add(prevStart);
                }
                record(Math.min(start, prevStart), Math.max(stop, prevStop), null);
                return;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "replace op boundaries of %d..%d overlap with previous %d..%d",
                                start, stop, prevStart, prevStop));
            }
        }
        Arrays.fill(coveredBy, start, stop + 1, start);
        replaceStop[start] = stop;
        replaceText[start] = text;
    }

    private void dropReplace(int start) {
        replacesWithInserts.remove(start);
        Arrays.fill(coveredBy, start, replaceStop[start] + 1, -1);
        replaceStop[start] = -1;
        replaceText[start] = null;
    }

    private String getText(int start, int stop) {
        if (!replacesWithInserts.isEmpty() || !orphanInserts.isEmpty()) {
            throw new IllegalArgumentException("insert op within boundaries of a replace");
        }
        start = Math.max(start, 0);
        stop = Math.min(stop, size - 1);
        StringBuilder sb = new StringBuilder();
        int i = start;
        while (i <= stop) {
            if (replaceStop[i] != -1) {
                appendInsertsReversed(sb, i);
                if (replaceText[i] != null) {
                    sb.append(replaceText[i]);
                }
                i = replaceStop[i] + 1;
            } else {
                appendInserts(sb, i);
                Token token = tokens.get(i);
                if (token.getType() != Token.EOF) {
                    sb.append(token.getText());
                }
                i++;
            }
        }
        if (stop == size - 1) {
            appendInserts(sb, size);
        }
        return sb.toString();
    }

    private void appendInserts(StringBuilder sb, int index) {
        List<Insert> list = inserts.get(index);
        if (list != null) {
            sb.append(combine(list));
        }
    }

    /** Combines inserts at one index the way {@code TokenStreamRewriter} does. */
    private static String combine(List<Insert> list) {
        StringBuilder combined = new StringBuilder(list.get(0).text);
        for (int j = 1; j < list.size(); j++) {
            String text = list.get(j).text;
            if (list.get(j - 1).after) {
                combined.append(text);
            } else {
                combined.insert(0, text);
            }
        }
        return combined.toString();
    }

    private void appendInsertsReversed(StringBuilder sb, int index) {
        List<Insert> list = inserts.get(index);
        if (list == null) {
            return;
        }
        for (int j = list.size() - 1; j >= 0; j--) {
            sb.append(list.get(j).text);
        }
    }

    private static final class Insert {
        final String text;
        final boolean after;

        Insert(String text, boolean after) {
            this.text = text;
            this.after = after;
        }
    }
}
