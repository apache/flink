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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.hint.QueryHintsRelShuttle;
import org.apache.flink.table.planner.hint.StateTtlHint;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
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
import static org.apache.flink.table.api.config.LookupJoinHintOptions.LOOKUP_TABLE;

/**
 * Resolve and validate the query hints.
 *
 * <p>Note: duplicate query hints are not checked here.
 *
 * <p>For KV hints such as state ttl hints and lookup join hints, they will be merged. If the keys
 * with same hint name conflict, only the first value is chosen.
 *
 * <p>For LIST hints such as regular join hints, they will all be retained.
 */
public class QueryHintsResolver extends QueryHintsRelShuttle {
    private final Set<RelHint> allHints = new HashSet<>();
    private final Set<RelHint> validHints = new HashSet<>();

    // hintName -> hintOptions -> whether this option has been checked
    private final Map<String, Map<String, Boolean>> allOptionsInQueryHints = new HashMap<>();

    /**
     * Resolves and validates query hints in the given {@link RelNode} list, an {@link
     * ValidationException} will be raised for invalid hints.
     *
     * <p>After resolving query hints, the options of the query hints (declared table name or query
     * block name) will be replaced to {@link FlinkHints#LEFT_INPUT} or {@link
     * FlinkHints#RIGHT_INPUT}
     *
     * <p>If the declared table name or query name in a query hint could not match the left side or
     * right side of this query, that means this query hint is invalid and a {@link
     * ValidationException} will be thrown.
     */
    final List<RelNode> resolve(List<RelNode> roots) {
        List<RelNode> resolvedRoots =
                roots.stream().map(node -> node.accept(this)).collect(Collectors.toList());
        validateHints();
        return resolvedRoots;
    }

    @Override
    protected RelNode doVisit(RelNode node) {
        List<RelHint> oldHints = ((Hintable) node).getHints();
        List<RelHint> oldQueryHints = FlinkHints.getAllQueryHints(oldHints);
        // has no hints, return directly.
        if (oldQueryHints.isEmpty()) {
            return super.visitChildren(node);
        }

        final List<RelHint> newHints;
        if (node instanceof BiRel) {
            BiRel biRel = (BiRel) node;
            Optional<String> leftName = extractAliasOrTableName(biRel.getLeft());
            Optional<String> rightName = extractAliasOrTableName(biRel.getRight());
            newHints = validateAndGetNewHints(leftName, rightName, oldHints);
        } else if (node instanceof SingleRel) {
            SingleRel singleRel = (SingleRel) node;
            Optional<String> tableName = extractAliasOrTableName(singleRel.getInput());
            newHints = validateAndGetNewHints(tableName, oldHints);
        } else {
            throw new TableException(
                    String.format(
                            "Unsupported node when resolving query hints: %s",
                            node.getClass().getCanonicalName()));
        }

        RelNode newNode = super.visitChildren(node);
        List<RelHint> mergedHints = mergeQueryHintsIfNecessary(newHints);
        // replace new query hints
        return ((Hintable) newNode).withHints(mergedHints);
    }

    /**
     * Resolve the query hints in the {@link BiRel} such as {@link
     * org.apache.calcite.rel.core.Correlate} and {@link org.apache.calcite.rel.core.Join}.
     *
     * @param leftName left table name, view name or alias name
     * @param rightName right table name, view name or alias name
     * @param oldHints old hints in this node
     */
    private List<RelHint> validateAndGetNewHints(
            Optional<String> leftName, Optional<String> rightName, List<RelHint> oldHints) {
        Set<RelHint> existentKVHints = new HashSet<>();

        List<RelHint> newHints = new ArrayList<>();
        for (RelHint hint : oldHints) {
            if (JoinStrategy.isLookupHint(hint.hintName)) {
                allHints.add(trimInheritPath(hint));
                Configuration conf = Configuration.fromMap(hint.kvOptions);
                // hint option checker has done the validation
                String lookupTable = conf.get(LOOKUP_TABLE);

                // add options about this hint for finally checking
                initOptionInfoAboutQueryHintsForCheck(
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
                initOptionInfoAboutQueryHintsForCheck(hint.hintName, hint.listOptions);

                // the declared table name or query block name is replaced by
                // FlinkHints#LEFT_INPUT or FlinkHints#RIGHT_INPUT
                List<String> newOptions =
                        getNewJoinHintOptions(leftName, rightName, hint.listOptions, hint.hintName);

                // check whether the join hint options are valid
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
            } else if (StateTtlHint.isStateTtlHint(hint.hintName)) {
                List<String> definedTables = new ArrayList<>(hint.kvOptions.keySet());
                initOptionInfoAboutQueryHintsForCheck(hint.hintName, definedTables);
                // the declared table name or query block name is replaced by
                // FlinkHints#LEFT_INPUT or FlinkHints#RIGHT_INPUT
                Map<String, String> newKvOptions =
                        getNewStateTtlHintOptions(
                                leftName, rightName, hint.kvOptions, hint.hintName);
                if (!newKvOptions.isEmpty()) {
                    // only accept a matched hint
                    validHints.add(trimInheritPath(hint));
                    newHints.add(RelHint.builder(hint.hintName).hintOptions(newKvOptions).build());
                }
            } else {
                if (!existentKVHints.contains(hint)) {
                    existentKVHints.add(hint);
                    newHints.add(hint);
                }
            }
        }
        return newHints;
    }

    /**
     * Resolve the query hints in the {@link SingleRel} such as {@link
     * org.apache.calcite.rel.core.Aggregate}.
     *
     * @param inputName the input table name, view name or alias name
     * @param oldHints old hints in this node
     */
    private List<RelHint> validateAndGetNewHints(
            Optional<String> inputName, List<RelHint> oldHints) {
        Set<RelHint> existentKVHints = new HashSet<>();

        List<RelHint> newHints = new ArrayList<>();

        for (RelHint hint : oldHints) {
            if (StateTtlHint.isStateTtlHint(hint.hintName)) {
                List<String> definedTables = new ArrayList<>(hint.kvOptions.keySet());
                initOptionInfoAboutQueryHintsForCheck(hint.hintName, definedTables);
                // the kv options will be converted to list options
                List<String> newListOptions =
                        getNewStateTtlHintOptions(inputName, hint.kvOptions, hint.hintName);
                if (!newListOptions.isEmpty()) {
                    // only accept a matched hint
                    validHints.add(trimInheritPath(hint));
                    newHints.add(
                            RelHint.builder(hint.hintName).hintOptions(newListOptions).build());
                }
            } else {
                if (!existentKVHints.contains(hint)) {
                    existentKVHints.add(hint);
                    newHints.add(hint);
                }
            }
        }
        return newHints;
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
                                                "Ambitious option: %s in join hint: %s, the input "
                                                        + "relations are: %s, %s",
                                                option, hintName, leftName, rightName));
                            } else if (leftName.isPresent()
                                    && matchIdentifier(option, leftName.get())) {
                                return FlinkHints.LEFT_INPUT;
                            } else if (rightName.isPresent()
                                    && matchIdentifier(option, rightName.get())) {
                                return FlinkHints.RIGHT_INPUT;
                            } else {
                                return "";
                            }
                        })
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());
    }

    private Map<String, String> getNewStateTtlHintOptions(
            Optional<String> leftName,
            Optional<String> rightName,
            Map<String, String> kvOptions,
            String hintName) {
        updateInfoForOptionCheck(hintName, leftName);
        updateInfoForOptionCheck(hintName, rightName);
        Map<String, String> newOptions = new HashMap<>();
        kvOptions.forEach(
                (input, ttl) -> {
                    if (leftName.isPresent()
                            && rightName.isPresent()
                            && matchIdentifier(input, leftName.get())
                            && matchIdentifier(input, rightName.get())) {
                        throw new ValidationException(
                                String.format(
                                        "Ambitious option: %s in state ttl hint: %s, the input "
                                                + "relations are: %s, %s",
                                        input, hintName, leftName, rightName));
                    } else if (leftName.isPresent() && matchIdentifier(input, leftName.get())) {
                        newOptions.put(FlinkHints.LEFT_INPUT, ttl);
                    } else if (rightName.isPresent() && matchIdentifier(input, rightName.get())) {
                        newOptions.put(FlinkHints.RIGHT_INPUT, ttl);
                    }
                });
        return newOptions;
    }

    /** The state ttl hint for {@link SingleRel} will be converted to a list option. */
    private List<String> getNewStateTtlHintOptions(
            Optional<String> inputName, Map<String, String> kvOptions, String hintName) {
        updateInfoForOptionCheck(hintName, inputName);
        return kvOptions.entrySet().stream()
                .filter(
                        entry ->
                                inputName.isPresent()
                                        && matchIdentifier(entry.getKey(), inputName.get()))
                .map(Map.Entry::getValue)
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
        for (String hintName : allOptionsInQueryHints.keySet()) {
            Map<String, Boolean> optionCheckedStatus = allOptionsInQueryHints.get(hintName);
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
                                        if (JoinStrategy.isLookupHint(hintName)
                                                || StateTtlHint.isStateTtlHint(hintName)) {
                                            // lookup join hint or state ttl hint
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

    private void initOptionInfoAboutQueryHintsForCheck(
            String hintName, List<String> definedTables) {
        if (allOptionsInQueryHints.containsKey(hintName)) {
            Map<String, Boolean> optionCheckedStatus = allOptionsInQueryHints.get(hintName);
            definedTables.forEach(
                    table -> {
                        if (!optionCheckedStatus.containsKey(table)) {
                            // all options are not checked when init
                            optionCheckedStatus.put(table, false);
                        }
                    });
        } else {
            allOptionsInQueryHints.put(
                    hintName,
                    new HashSet<>(definedTables)
                            .stream()
                                    // all options are not checked when init
                                    .collect(Collectors.toMap(table -> table, table -> false)));
        }
    }

    private void updateInfoForOptionCheck(String hintName, Optional<String> tableName) {
        if (tableName.isPresent()) {
            Map<String, Boolean> optionMapper = allOptionsInQueryHints.get(hintName);
            for (String option : optionMapper.keySet()) {
                if (matchIdentifier(option, tableName.get())) {
                    // if the hint has not been checked before, update it
                    allOptionsInQueryHints.get(hintName).put(option, true);
                }
            }
        }
    }

    /**
     * For KV hint like state ttl hint or lookup join hint, we need to merge the hints if there are
     * multiple hints and choose the first value with same key.
     */
    private List<RelHint> mergeQueryHintsIfNecessary(List<RelHint> hints) {
        List<RelHint> result = new ArrayList<>();
        Map<String, Map<String, String>> kvHintsMap = new HashMap<>();
        Map<String, String> listHintsMap = new HashMap<>();

        for (RelHint hint : hints) {
            String hintName = hint.hintName;

            // if the hint is a join hint or alias hint, add it directly
            if (JoinStrategy.isJoinStrategy(hintName) || FlinkHints.isAliasHint(hintName)) {
                result.add(hint);
                continue;
            }

            if (!hint.kvOptions.isEmpty()) {
                // if the hint is KV hint like lookup hint and state ttl hint on BiRel, merge it
                // with the existing hints
                Map<String, String> kvOptions = new HashMap<>(hint.kvOptions);
                if (kvHintsMap.containsKey(hintName)) {
                    Map<String, String> existingOptions = kvHintsMap.get(hintName);
                    for (String key : kvOptions.keySet()) {
                        // if the key is same, choose the first hint to take effect
                        existingOptions.computeIfAbsent(key, k -> kvOptions.get(key));
                    }
                } else {
                    kvHintsMap.put(hintName, kvOptions);
                }
            } else if (!hint.listOptions.isEmpty()) {
                // if the hint is LIST hint like state ttl hint on SingleRel, choose the first hint
                // to take effect
                listHintsMap.computeIfAbsent(hintName, k -> hint.listOptions.get(0));
            } else {
                // throw an exception again although empty options may be checked by different
                // checkers in FlinkHintStrategies before
                throw new ValidationException(
                        String.format(
                                "Invalid %s hint, the key-value options and list options are all empty",
                                hintName));
            }
        }

        for (String kvHintName : kvHintsMap.keySet()) {
            result.add(RelHint.builder(kvHintName).hintOptions(kvHintsMap.get(kvHintName)).build());
        }
        for (String listHintName : listHintsMap.keySet()) {
            result.add(
                    RelHint.builder(listHintName)
                            .hintOptions(Collections.singletonList(listHintsMap.get(listHintName)))
                            .build());
        }
        return result;
    }
}
