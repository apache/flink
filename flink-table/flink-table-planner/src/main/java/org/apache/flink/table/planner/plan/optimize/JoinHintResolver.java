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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.hint.JoinStrategy;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.planner.hint.LookupJoinHintOptions.LOOKUP_TABLE;

/**
 * Resolve and validate the join hints.
 *
 * <p>Note: duplicate join hints are not checked here.
 */
public class JoinHintResolver extends RelShuttleImpl {
    private final Set<RelHint> allHints = new HashSet<>();
    private final Set<RelHint> validHints = new HashSet<>();

    // hintName -> hintOptions -> whether this option has been checked
    private final Map<String, Map<String, Boolean>> allOptionsInJoinHints = new HashMap<>();

    /**
     * Resolves and validates join hints in the given {@link RelNode} list, an {@link
     * ValidationException} will be raised for invalid hints.
     *
     * <p>After resolving join hints, the options of the join hints (declared table name or query
     * block name) will be replaced to {@link JoinStrategy#LEFT_INPUT} or {@link
     * JoinStrategy#RIGHT_INPUT}
     *
     * <p>If the declared table name or query name in a join hint could not match the left side or
     * right side of this join, that means this join hint is invalid and a {@link
     * ValidationException} will be thrown.
     */
    final List<RelNode> resolve(List<RelNode> roots) {
        List<RelNode> resolvedRoots =
                roots.stream().map(node -> node.accept(this)).collect(Collectors.toList());
        validateHints();
        return resolvedRoots;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return visitBiRel(join);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return visitBiRel(correlate);
    }

    private RelNode visitBiRel(BiRel biRel) {
        Optional<String> leftName = extractAliasOrTableName(biRel.getLeft());
        Optional<String> rightName = extractAliasOrTableName(biRel.getRight());

        Set<RelHint> existentKVHints = new HashSet<>();

        List<RelHint> oldHints = ((Hintable) biRel).getHints();
        List<RelHint> newHints = new ArrayList<>();

        for (RelHint hint : oldHints) {
            if (JoinStrategy.isLookupHint(hint.hintName)) {
                allHints.add(trimInheritPath(hint));
                Configuration conf = Configuration.fromMap(hint.kvOptions);
                // hint option checker has done the validation
                String lookupTable = conf.get(LOOKUP_TABLE);

                // add options about this hint for finally checking
                initOptionInfoAboutJoinHintsForCheck(
                        hint.hintName, Collections.singletonList(lookupTable));

                assert null != lookupTable;
                if (rightName.isPresent() && matchIdentifier(lookupTable, rightName.get())) {
                    validHints.add(trimInheritPath(hint));
                    updateInfoForOptionCheck(hint.hintName, rightName);
                    newHints.add(hint);
                }
            } else if (JoinStrategy.isJoinStrategy(hint.hintName)) {
                allHints.add(trimInheritPath(hint));
                // add options about this hint for finally checking
                initOptionInfoAboutJoinHintsForCheck(hint.hintName, hint.listOptions);

                // the declared table name or query block name is replaced by
                // JoinStrategy#LEFT_INPUT or JoinStrategy#RIGHT_INPUT
                List<String> newOptions =
                        getNewJoinHintOptions(leftName, rightName, hint.listOptions, hint.hintName);

                // check whether the join hints options are valid
                boolean isValidOption = JoinStrategy.validOptions(hint.hintName, newOptions);
                if (isValidOption) {
                    validHints.add(trimInheritPath(hint));
                    // if the hint defines more than one args, only
                    // retain the first one
                    newHints.add(
                            RelHint.builder(hint.hintName)
                                    .hintOptions(singletonList(newOptions.get(0)))
                                    .build());
                }
            } else {
                if (!existentKVHints.contains(hint)) {
                    existentKVHints.add(hint);
                    newHints.add(hint);
                }
            }
        }

        RelNode newNode = super.visitChildren(biRel);

        List<RelHint> oldJoinHints = FlinkHints.getAllJoinHints(oldHints);
        if (!oldJoinHints.isEmpty()) {
            // replace the table name as LEFT or RIGHT
            return ((Hintable) newNode).withHints(newHints);
        }
        // has no hints, return original node directly.
        return newNode;
    }

    private List<String> getNewJoinHintOptions(
            Optional<String> leftName,
            Optional<String> rightName,
            List<String> listOptions,
            String hintName) {

        // update info about 'allOptionsInJoinHints' for checking finally
        updateInfoForOptionCheck(hintName, leftName);
        updateInfoForOptionCheck(hintName, rightName);

        return listOptions.stream()
                .map(
                        option -> {
                            if (leftName.isPresent()
                                    && rightName.isPresent()
                                    && matchIdentifier(option, leftName.get())
                                    && matchIdentifier(option, rightName.get())) {
                                throw new ValidationException(
                                        String.format(
                                                "Ambitious option: %s in hint: %s, the input "
                                                        + "relations are: %s, %s",
                                                option, hintName, leftName, rightName));
                            } else if (leftName.isPresent()
                                    && matchIdentifier(option, leftName.get())) {
                                return JoinStrategy.LEFT_INPUT;
                            } else if (rightName.isPresent()
                                    && matchIdentifier(option, rightName.get())) {
                                return JoinStrategy.RIGHT_INPUT;
                            } else {
                                return "";
                            }
                        })
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());
    }

    private void validateHints() {
        Set<RelHint> invalidHints = new HashSet<>(allHints);
        invalidHints.removeAll(validHints);
        String errorPattern;

        // firstly, check the unknown table (view) names in hints
        errorPattern =
                "The options of following hints cannot match the name of "
                        + "input tables or views: %s";
        StringBuilder errorMsgSb = new StringBuilder();
        AtomicBoolean containsInvalidOptions = new AtomicBoolean(false);
        for (String hintName : allOptionsInJoinHints.keySet()) {
            Map<String, Boolean> optionCheckedStatus = allOptionsInJoinHints.get(hintName);
            errorMsgSb.append("\n");
            errorMsgSb.append(
                    String.format(
                            "`%s` in `%s`",
                            optionCheckedStatus.keySet().stream()
                                    .filter(
                                            op -> {
                                                boolean checked = !optionCheckedStatus.get(op);
                                                if (checked) {
                                                    containsInvalidOptions.set(true);
                                                }
                                                return checked;
                                            })
                                    .collect(Collectors.joining(", ")),
                            hintName));
        }
        if (containsInvalidOptions.get()) {
            throw new ValidationException(String.format(errorPattern, errorMsgSb));
        }

        // secondly, check invalid hints.
        // see more at JoinStrategy#validOptions
        if (!invalidHints.isEmpty()) {
            errorPattern = "The options of following hints is invalid: %s";
            String errorMsg =
                    invalidHints.stream()
                            .map(
                                    hint -> {
                                        String hintName = hint.hintName;
                                        if (JoinStrategy.isLookupHint(hintName)) {
                                            // lookup join
                                            return hint.hintName;
                                        } else {
                                            // join hint
                                            return hint.hintName
                                                    + " :"
                                                    + StringUtils.join(hint.listOptions, ", ");
                                        }
                                    })
                            .collect(Collectors.joining("\n", "\n", ""));
            throw new ValidationException(String.format(errorPattern, errorMsg));
        }
    }

    private RelHint trimInheritPath(RelHint hint) {
        RelHint.Builder builder = RelHint.builder(hint.hintName);
        if (hint.listOptions.isEmpty()) {
            return builder.hintOptions(hint.kvOptions).build();
        } else {
            return builder.hintOptions(hint.listOptions).build();
        }
    }

    private Optional<String> extractAliasOrTableName(RelNode node) {
        // check whether the input relation is converted from a view
        Optional<String> aliasName = FlinkHints.getTableAlias(node);
        if (aliasName.isPresent()) {
            return aliasName;
        }
        // otherwise, the option may be a table name
        Optional<TableScan> tableScan = getTableScan(node);
        if (tableScan.isPresent()) {
            Optional<String> tableName = FlinkHints.getTableName(tableScan.get().getTable());
            if (tableName.isPresent()) {
                return tableName;
            }
        }

        return Optional.empty();
    }

    private Optional<TableScan> getTableScan(RelNode node) {
        if (node instanceof TableScan) {
            return Optional.of((TableScan) node);
        } else {
            if (FlinkHints.canTransposeToTableScan(node)) {
                return getTableScan(node.getInput(0));
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Check whether the given hint option matches the table qualified names. For convenience, we
     * follow a simple rule: the matching is successful if the option is the suffix of the table
     * qualified names.
     */
    private boolean matchIdentifier(String option, String tableIdentifier) {
        String[] optionNames = option.split("\\.");
        int optionNameLength = optionNames.length;

        String[] tableNames = tableIdentifier.split("\\.");
        int tableNameLength = tableNames.length;

        for (int i = 0; i < Math.min(optionNameLength, tableNameLength); i++) {
            String currOptionName = optionNames[optionNameLength - 1 - i];
            String currTableName = tableNames[tableNameLength - 1 - i];

            if (!currOptionName.equals(currTableName)) {
                return false;
            }
        }

        return true;
    }

    private void initOptionInfoAboutJoinHintsForCheck(String hintName, List<String> definedTables) {
        if (allOptionsInJoinHints.containsKey(hintName)) {
            Map<String, Boolean> optionCheckedStatus = allOptionsInJoinHints.get(hintName);
            definedTables.forEach(
                    table -> {
                        if (!optionCheckedStatus.containsKey(table)) {
                            // all options are not checked when init
                            optionCheckedStatus.put(table, false);
                        }
                    });
        } else {
            allOptionsInJoinHints.put(
                    hintName,
                    new HashSet<>(definedTables)
                            .stream()
                                    // all options are not checked when init
                                    .collect(Collectors.toMap(table -> table, table -> false)));
        }
    }

    private void updateInfoForOptionCheck(String hintName, Optional<String> tableName) {
        if (tableName.isPresent()) {
            Map<String, Boolean> optionMapper = allOptionsInJoinHints.get(hintName);
            for (String option : optionMapper.keySet()) {
                if (matchIdentifier(option, tableName.get())) {
                    // if the hint has not been checked before, update it
                    allOptionsInJoinHints.get(hintName).put(option, true);
                }
            }
        }
    }
}
