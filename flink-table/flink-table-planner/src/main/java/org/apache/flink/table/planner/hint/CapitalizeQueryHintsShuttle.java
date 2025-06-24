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

package org.apache.flink.table.planner.hint;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** A shuttle to capitalize all query hints on corresponding nodes. */
public class CapitalizeQueryHintsShuttle extends QueryHintsRelShuttle {

    @Override
    protected RelNode doVisit(RelNode node) {
        Hintable hNode = (Hintable) node;
        AtomicBoolean changed = new AtomicBoolean(false);
        List<RelHint> hintsWithCapitalJoinHints =
                hNode.getHints().stream()
                        .map(
                                hint -> {
                                    String capitalHintName = hint.hintName.toUpperCase(Locale.ROOT);
                                    if (!FlinkHints.isQueryHint(capitalHintName)
                                            || hint.hintName.equals(capitalHintName)) {
                                        return hint;
                                    }

                                    changed.set(true);
                                    if (JoinStrategy.isJoinStrategy(capitalHintName)) {
                                        if (JoinStrategy.isLookupHint(hint.hintName)) {
                                            return RelHint.builder(capitalHintName)
                                                    .hintOptions(hint.kvOptions)
                                                    .inheritPath(hint.inheritPath)
                                                    .build();
                                        }
                                        return RelHint.builder(capitalHintName)
                                                .hintOptions(hint.listOptions)
                                                .inheritPath(hint.inheritPath)
                                                .build();
                                    } else if (StateTtlHint.isStateTtlHint(hint.hintName)) {
                                        return RelHint.builder(capitalHintName)
                                                .hintOptions(hint.kvOptions)
                                                .inheritPath(hint.inheritPath)
                                                .build();
                                    }
                                    throw new IllegalStateException(
                                            "Unknown hint: " + hint.hintName);
                                })
                        .collect(Collectors.toList());

        if (changed.get()) {
            return super.visit(hNode.withHints(hintsWithCapitalJoinHints));
        } else {
            return super.visit(node);
        }
    }
}
