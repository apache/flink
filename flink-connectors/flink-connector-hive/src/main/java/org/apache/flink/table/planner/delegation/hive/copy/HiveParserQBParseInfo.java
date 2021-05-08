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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.QBParseInfo. */
public class HiveParserQBParseInfo {

    private final boolean isSubQ;
    private String alias;
    private HiveParserASTNode joinExpr;
    private HiveParserASTNode hints;
    private final HashMap<String, HiveParserASTNode> aliasToSrc;
    // insclause-0 -> TOK_TAB HiveParserASTNode
    private final HashMap<String, HiveParserASTNode> nameToDest;
    /** For 'insert into FOO(x,y) select ...' this stores the insclause-0 -> x,y mapping. */
    private final Map<String, List<String>> nameToDestSchema;

    private final Set<String> tableSampleAliases;
    private final Map<HiveParserASTNode, String> exprToColumnAlias;
    private final Map<String, HiveParserASTNode> destToSelExpr;
    private final HashMap<String, HiveParserASTNode> destToWhereExpr;
    private final HashMap<String, HiveParserASTNode> destToGroupby;
    private final Set<String> destRollups;
    private final Set<String> destCubes;
    private final Set<String> destGroupingSets;
    private final Map<String, HiveParserASTNode> destToHaving;
    // insertIntoTables/insertOverwriteTables map a table's fullName to its ast;
    private final Map<String, HiveParserASTNode> insertIntoTables;
    private final Map<String, HiveParserASTNode> insertOverwriteTables;
    private HiveParserASTNode queryFromExpr;

    private boolean isAnalyzeCommand; // used for the analyze command (statistics)
    private boolean isNoScanAnalyzeCommand; // used for the analyze command (statistics) (noscan)
    private boolean isPartialScanAnalyzeCommand; // used for the analyze command (statistics)
    // (partialscan)

    private final HashMap<String, HiveParserBaseSemanticAnalyzer.TableSpec>
            tableSpecs; // used for statistics

    /** ClusterBy is a short name for both DistributeBy and SortBy. */
    private final HashMap<String, HiveParserASTNode> destToClusterby;
    /**
     * DistributeBy controls the hashcode of the row, which determines which reducer the rows will
     * go to.
     */
    private final HashMap<String, HiveParserASTNode> destToDistributeby;
    /**
     * SortBy controls the reduce keys, which affects the order of rows that the reducer receives.
     */
    private final HashMap<String, HiveParserASTNode> destToSortby;

    /** Maping from table/subquery aliases to all the associated lateral view nodes. */
    private final HashMap<String, ArrayList<HiveParserASTNode>> aliasToLateralViews;

    private final HashMap<String, HiveParserASTNode> destToLateralView;

    /* Order by clause */
    private final HashMap<String, HiveParserASTNode> destToOrderby;
    // Use SimpleEntry to save the offset and rowcount of limit clause
    // KEY of SimpleEntry: offset
    // VALUE of SimpleEntry: rowcount
    private final HashMap<String, AbstractMap.SimpleEntry<Integer, Integer>> destToLimit;
    private final int outerQueryLimit;

    // used by GroupBy
    private final LinkedHashMap<String, LinkedHashMap<String, HiveParserASTNode>>
            destToAggregationExprs;
    private final HashMap<String, List<HiveParserASTNode>> destToDistinctFuncExprs;

    // used by Windowing
    private final LinkedHashMap<String, LinkedHashMap<String, HiveParserASTNode>>
            destToWindowingExprs;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(HiveParserQBParseInfo.class);

    public HiveParserQBParseInfo(String alias, boolean isSubQ) {
        aliasToSrc = new HashMap<>();
        nameToDest = new HashMap<>();
        nameToDestSchema = new HashMap<>();
        tableSampleAliases = new HashSet<>();
        exprToColumnAlias = new HashMap<>();
        destToLateralView = new HashMap<>();
        destToSelExpr = new LinkedHashMap<>();
        destToWhereExpr = new HashMap<>();
        destToGroupby = new HashMap<>();
        destToHaving = new HashMap<>();
        destToClusterby = new HashMap<>();
        destToDistributeby = new HashMap<>();
        destToSortby = new HashMap<>();
        destToOrderby = new HashMap<>();
        destToLimit = new HashMap<>();
        insertIntoTables = new HashMap<>();
        insertOverwriteTables = new HashMap<>();
        destRollups = new HashSet<>();
        destCubes = new HashSet<>();
        destGroupingSets = new HashSet<>();

        destToAggregationExprs = new LinkedHashMap<>();
        destToWindowingExprs = new LinkedHashMap<>();
        destToDistinctFuncExprs = new HashMap<>();

        this.alias = alias;
        this.isSubQ = isSubQ;
        outerQueryLimit = -1;

        aliasToLateralViews = new HashMap<>();

        tableSpecs = new HashMap<>();
    }

    public void setAggregationExprsForClause(
            String clause, LinkedHashMap<String, HiveParserASTNode> aggregationTrees) {
        destToAggregationExprs.put(clause, aggregationTrees);
    }

    public void addAggregationExprsForClause(
            String clause, LinkedHashMap<String, HiveParserASTNode> aggregationTrees) {
        if (destToAggregationExprs.containsKey(clause)) {
            destToAggregationExprs.get(clause).putAll(aggregationTrees);
        } else {
            destToAggregationExprs.put(clause, aggregationTrees);
        }
    }

    public void addInsertIntoTable(String fullName, HiveParserASTNode ast) {
        insertIntoTables.put(fullName.toLowerCase(), ast);
    }

    // See also {@link #getInsertOverwriteTables()}
    public boolean isInsertIntoTable(String dbName, String table) {
        String fullName = dbName + "." + table;
        return insertIntoTables.containsKey(fullName.toLowerCase());
    }

    public HashMap<String, HiveParserASTNode> getAggregationExprsForClause(String clause) {
        return destToAggregationExprs.get(clause);
    }

    public void addWindowingExprToClause(String clause, HiveParserASTNode windowingExprNode) {
        LinkedHashMap<String, HiveParserASTNode> windowingExprs =
                destToWindowingExprs.computeIfAbsent(
                        clause, k -> new LinkedHashMap<String, HiveParserASTNode>());
        windowingExprs.put(windowingExprNode.toStringTree(), windowingExprNode);
    }

    public HashMap<String, HiveParserASTNode> getWindowingExprsForClause(String clause) {
        return destToWindowingExprs.get(clause);
    }

    public void setDistinctFuncExprsForClause(String clause, List<HiveParserASTNode> ast) {
        destToDistinctFuncExprs.put(clause, ast);
    }

    public List<HiveParserASTNode> getDistinctFuncExprsForClause(String clause) {
        return destToDistinctFuncExprs.get(clause);
    }

    public void setSelExprForClause(String clause, HiveParserASTNode ast) {
        destToSelExpr.put(clause, ast);
    }

    public void setQueryFromExpr(HiveParserASTNode ast) {
        queryFromExpr = ast;
    }

    public void setWhrExprForClause(String clause, HiveParserASTNode ast) {
        destToWhereExpr.put(clause, ast);
    }

    public void setHavingExprForClause(String clause, HiveParserASTNode ast) {
        destToHaving.put(clause, ast);
    }

    public void setGroupByExprForClause(String clause, HiveParserASTNode ast) {
        destToGroupby.put(clause, ast);
    }

    public void setDestForClause(String clause, HiveParserASTNode ast) {
        nameToDest.put(clause, ast);
    }

    List<String> setDestSchemaForClause(String clause, List<String> columnList) {
        return nameToDestSchema.put(clause, columnList);
    }

    public List<String> getDestSchemaForClause(String clause) {
        return nameToDestSchema.get(clause);
    }

    /** Set the Cluster By AST for the clause. */
    public void setClusterByExprForClause(String clause, HiveParserASTNode ast) {
        destToClusterby.put(clause, ast);
    }

    /** Set the Distribute By AST for the clause. */
    public void setDistributeByExprForClause(String clause, HiveParserASTNode ast) {
        destToDistributeby.put(clause, ast);
    }

    /** Set the Sort By AST for the clause. */
    public void setSortByExprForClause(String clause, HiveParserASTNode ast) {
        destToSortby.put(clause, ast);
    }

    public void setOrderByExprForClause(String clause, HiveParserASTNode ast) {
        destToOrderby.put(clause, ast);
    }

    public void setSrcForAlias(String alias, HiveParserASTNode ast) {
        aliasToSrc.put(alias.toLowerCase(), ast);
    }

    public Set<String> getClauseNames() {
        return destToSelExpr.keySet();
    }

    public Set<String> getClauseNamesForDest() {
        return nameToDest.keySet();
    }

    public HiveParserASTNode getDestForClause(String clause) {
        return nameToDest.get(clause);
    }

    public HashMap<String, HiveParserASTNode> getDestToWhereExpr() {
        return destToWhereExpr;
    }

    public HiveParserASTNode getGroupByForClause(String clause) {
        return destToGroupby.get(clause);
    }

    public Set<String> getDestRollups() {
        return destRollups;
    }

    public Set<String> getDestCubes() {
        return destCubes;
    }

    public Set<String> getDestGroupingSets() {
        return destGroupingSets;
    }

    public HashMap<String, HiveParserASTNode> getDestToGroupBy() {
        return destToGroupby;
    }

    public HiveParserASTNode getHavingForClause(String clause) {
        return destToHaving.get(clause);
    }

    public HiveParserASTNode getSelForClause(String clause) {
        return destToSelExpr.get(clause);
    }

    public HiveParserASTNode getQueryFrom() {
        return queryFromExpr;
    }

    /** Get the Cluster By AST for the clause. */
    public HiveParserASTNode getClusterByForClause(String clause) {
        return destToClusterby.get(clause);
    }

    public HashMap<String, HiveParserASTNode> getDestToClusterBy() {
        return destToClusterby;
    }

    /** Get the Distribute By AST for the clause. */
    public HiveParserASTNode getDistributeByForClause(String clause) {
        return destToDistributeby.get(clause);
    }

    public HashMap<String, HiveParserASTNode> getDestToDistributeBy() {
        return destToDistributeby;
    }

    /** Get the Sort By AST for the clause. */
    public HiveParserASTNode getSortByForClause(String clause) {
        return destToSortby.get(clause);
    }

    public HiveParserASTNode getOrderByForClause(String clause) {
        return destToOrderby.get(clause);
    }

    public HashMap<String, HiveParserASTNode> getDestToSortBy() {
        return destToSortby;
    }

    public HashMap<String, HiveParserASTNode> getDestToOrderBy() {
        return destToOrderby;
    }

    public HiveParserASTNode getSrcForAlias(String alias) {
        return aliasToSrc.get(alias.toLowerCase());
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public boolean getIsSubQ() {
        return isSubQ;
    }

    public HiveParserASTNode getJoinExpr() {
        return joinExpr;
    }

    public void setJoinExpr(HiveParserASTNode joinExpr) {
        this.joinExpr = joinExpr;
    }

    public boolean needTableSample(String alias) {
        return tableSampleAliases.contains(alias.toLowerCase());
    }

    public void setTabSample(String alias) {
        tableSampleAliases.add(alias.toLowerCase());
    }

    public Map<HiveParserASTNode, String> getAllExprToColumnAlias() {
        return exprToColumnAlias;
    }

    public void setExprToColumnAlias(HiveParserASTNode expr, String alias) {
        exprToColumnAlias.put(expr, alias);
    }

    public void setDestLimit(String dest, Integer offset, Integer limit) {
        destToLimit.put(dest, new AbstractMap.SimpleEntry<>(offset, limit));
    }

    public Integer getDestLimit(String dest) {
        return destToLimit.get(dest) == null ? null : destToLimit.get(dest).getValue();
    }

    // for fast check of possible existence of RS (will be checked again in SimpleFetchOptimizer)
    public boolean isSimpleSelectQuery() {
        if (joinExpr != null
                || !destToOrderby.isEmpty()
                || !destToSortby.isEmpty()
                || !destToGroupby.isEmpty()
                || !destToClusterby.isEmpty()
                || !destToDistributeby.isEmpty()
                || !destRollups.isEmpty()
                || !destCubes.isEmpty()
                || !destGroupingSets.isEmpty()
                || !destToHaving.isEmpty()) {
            return false;
        }

        for (Map<String, HiveParserASTNode> entry : destToAggregationExprs.values()) {
            if (entry != null && !entry.isEmpty()) {
                return false;
            }
        }

        for (Map<String, HiveParserASTNode> entry : destToWindowingExprs.values()) {
            if (entry != null && !entry.isEmpty()) {
                return false;
            }
        }

        for (List<HiveParserASTNode> ct : destToDistinctFuncExprs.values()) {
            if (!ct.isEmpty()) {
                return false;
            }
        }

        // exclude insert queries
        for (HiveParserASTNode v : nameToDest.values()) {
            if (!(v.getChild(0).getType() == HiveASTParser.TOK_TMP_FILE)) {
                return false;
            }
        }

        return true;
    }

    public void setHints(HiveParserASTNode hint) {
        hints = hint;
    }

    public HiveParserASTNode getHints() {
        return hints;
    }

    public Map<String, ArrayList<HiveParserASTNode>> getAliasToLateralViews() {
        return aliasToLateralViews;
    }

    public void addLateralViewForAlias(String alias, HiveParserASTNode lateralView) {
        ArrayList<HiveParserASTNode> lateralViews =
                aliasToLateralViews.computeIfAbsent(alias, k -> new ArrayList<>());
        lateralViews.add(lateralView);
    }

    public void setIsAnalyzeCommand(boolean isAnalyzeCommand) {
        this.isAnalyzeCommand = isAnalyzeCommand;
    }

    public boolean isAnalyzeCommand() {
        return isAnalyzeCommand;
    }

    public void addTableSpec(String tName, HiveParserBaseSemanticAnalyzer.TableSpec tSpec) {
        tableSpecs.put(tName, tSpec);
    }

    public HashMap<String, AbstractMap.SimpleEntry<Integer, Integer>> getDestToLimit() {
        return destToLimit;
    }

    public HashMap<String, HiveParserASTNode> getDestToLateralView() {
        return destToLateralView;
    }

    /** @param isNoScanAnalyzeCommand the isNoScanAnalyzeCommand to set */
    public void setNoScanAnalyzeCommand(boolean isNoScanAnalyzeCommand) {
        this.isNoScanAnalyzeCommand = isNoScanAnalyzeCommand;
    }

    /** @return the isPartialScanAnalyzeCommand */
    public boolean isPartialScanAnalyzeCommand() {
        return isPartialScanAnalyzeCommand;
    }

    /** @param isPartialScanAnalyzeCommand the isPartialScanAnalyzeCommand to set */
    public void setPartialScanAnalyzeCommand(boolean isPartialScanAnalyzeCommand) {
        this.isPartialScanAnalyzeCommand = isPartialScanAnalyzeCommand;
    }

    // See also {@link #isInsertIntoTable(String)}
    public Map<String, HiveParserASTNode> getInsertOverwriteTables() {
        return insertOverwriteTables;
    }
}
