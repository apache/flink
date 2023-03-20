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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.delegation.hive.HiveParserQBMetaData;

import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.QB. */
public class HiveParserQB {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserQB.class);

    private int numSels = 0;
    private int numSelDi = 0;
    private final Map<String, String> aliasToTabsOriginName;
    private final HashMap<String, String> aliasToTabs;
    private final HashMap<String, HiveParserQBExpr> aliasToSubq;
    private final HashMap<String, CatalogView> viewAliasToViewSchema;
    private final HashMap<String, Map<String, String>> aliasToProps;
    private final List<String> aliases;
    private final HiveParserQBParseInfo qbp;
    private final HiveParserQBMetaData qbm;
    private final String id;
    private boolean isQuery;
    private boolean insideView;
    private Set<String> aliasInsideView;
    // tableName -> <catalogTable, valuesData>
    private final Map<String, Tuple2<ResolvedCatalogTable, List<List<String>>>> valuesTableToData =
            new HashMap<>();

    // used by PTFs
    /*
     * This map maintains the HiveParserPTFInvocationSpec for each PTF chain invocation in this QB.
     */
    private HashMap<HiveParserASTNode, HiveParserPTFInvocationSpec> ptfNodeToSpec;
    /*
     * the HiveParserWindowingSpec used for windowing clauses in this QB.
     */
    private final HashMap<String, HiveParserWindowingSpec> destToWindowingSpec;

    /*
     * used to give a unique name to each SubQuery QB Currently there can be at
     * most 2 SubQueries in a Query: 1 in the Where clause, and 1 in the Having
     * clause.
     */
    private int numSubQueryPredicates;

    private CreateTableDesc createTableDesc;

    private HiveParserDirectoryDesc directoryDesc;

    public void print(String msg) {
        LOG.info(msg + "alias=" + qbp.getAlias());
        for (String alias : getSubqAliases()) {
            HiveParserQBExpr qbexpr = getSubqForAlias(alias);
            LOG.info(msg + "start subquery " + alias);
            qbexpr.print(msg + " ");
            LOG.info(msg + "end subquery " + alias);
        }
    }

    public HiveParserQB(String outerId, String alias, boolean isSubQ) {
        // Must be deterministic order maps - see HIVE-8707
        aliasToTabsOriginName = new LinkedHashMap<>();
        aliasToTabs = new LinkedHashMap<>();
        aliasToSubq = new LinkedHashMap<>();
        viewAliasToViewSchema = new LinkedHashMap<>();
        aliasToProps = new LinkedHashMap<>();
        aliases = new ArrayList<>();
        if (alias != null) {
            alias = alias.toLowerCase();
        }
        qbp = new HiveParserQBParseInfo(alias, isSubQ);
        qbm = new HiveParserQBMetaData();
        // Must be deterministic order maps - see HIVE-8707
        ptfNodeToSpec = new LinkedHashMap<>();
        destToWindowingSpec = new LinkedHashMap<>();
        id = getAppendedAliasFromId(outerId, alias);
        aliasInsideView = new HashSet<>();
    }

    // For sub-queries, the id. and alias should be appended since same aliases can be re-used
    // within different sub-queries.
    // For a query like:
    // select ...
    //   (select * from T1 a where ...) subq1
    //  join
    //   (select * from T2 a where ...) subq2
    // ..
    // the alias is modified to subq1:a and subq2:a from a, to identify the right sub-query.
    private static String getAppendedAliasFromId(String outerId, String alias) {
        return (outerId == null ? alias : outerId + ":" + alias);
    }

    public HiveParserQBParseInfo getParseInfo() {
        return qbp;
    }

    public HiveParserQBMetaData getMetaData() {
        return qbm;
    }

    public void countSelDi() {
        numSelDi++;
    }

    public void countSel() {
        numSels++;
    }

    public boolean exists(String alias) {
        alias = alias.toLowerCase();
        return aliasToTabs.get(alias) != null || aliasToSubq.get(alias) != null;
    }

    /**
     * Maintain table alias -> (originTableName, qualifiedName).
     *
     * @param alias table alias
     * @param originTableName table name that be actually specified, may be "table", "db.table",
     *     "catalog.db.table"
     * @param qualifiedName table name with full path, always is "catalog.db.table"
     */
    public void setTabAlias(String alias, String originTableName, String qualifiedName) {
        aliasToTabsOriginName.put(alias.toLowerCase(), originTableName.toLowerCase());
        aliasToTabs.put(alias.toLowerCase(), qualifiedName);
    }

    public void setSubqAlias(String alias, HiveParserQBExpr qbexpr) {
        aliasToSubq.put(alias.toLowerCase(), qbexpr);
    }

    public void setTabProps(String alias, Map<String, String> props) {
        aliasToProps.put(alias.toLowerCase(), props);
    }

    public void addAlias(String alias) {
        if (!aliases.contains(alias.toLowerCase())) {
            aliases.add(alias.toLowerCase());
        }
    }

    public String getId() {
        return id;
    }

    public int getNumSels() {
        return numSels;
    }

    public Set<String> getSubqAliases() {
        return aliasToSubq.keySet();
    }

    public Set<String> getTabAliases() {
        return aliasToTabs.keySet();
    }

    public List<String> getAliases() {
        return aliases;
    }

    public HiveParserQBExpr getSubqForAlias(String alias) {
        return aliasToSubq.get(alias.toLowerCase());
    }

    public String getTabNameForAlias(String alias) {
        return aliasToTabs.get(alias.toLowerCase());
    }

    public String getOriginTabNameForAlias(String alias) {
        return aliasToTabsOriginName.get(alias.toLowerCase());
    }

    public void rewriteViewToSubq(
            String alias, String viewName, HiveParserQBExpr qbexpr, CatalogView view) {
        alias = alias.toLowerCase();
        String tableName = aliasToTabs.remove(alias);
        String originTableName = aliasToTabsOriginName.remove(alias);
        assert (viewName.equals(tableName) || viewName.equals(originTableName));
        aliasToSubq.put(alias, qbexpr);
        if (view != null) {
            viewAliasToViewSchema.put(alias, view);
        }
    }

    public void rewriteCTEToSubq(String alias, String cteName, HiveParserQBExpr qbexpr) {
        rewriteViewToSubq(alias, cteName, qbexpr, null);
    }

    public void setIsQuery(boolean isQuery) {
        this.isQuery = isQuery;
    }

    /**
     * Set to true in SemanticAnalyzer.getMetadataForDestFile, if destination is a file and query is
     * not CTAS.
     */
    public boolean getIsQuery() {
        return isQuery;
    }

    // to find target for fetch task conversion optimizer (not allows subqueries)
    public boolean isSimpleSelectQuery() {
        if (!qbp.isSimpleSelectQuery() || isCTAS() || qbp.isAnalyzeCommand()) {
            return false;
        }
        for (HiveParserQBExpr qbexpr : aliasToSubq.values()) {
            if (!qbexpr.isSimpleSelectQuery()) {
                return false;
            }
        }
        return true;
    }

    public CreateTableDesc getTableDesc() {
        return createTableDesc;
    }

    public void setDirectoryDesc(HiveParserDirectoryDesc directoryDesc) {
        this.directoryDesc = directoryDesc;
    }

    public HiveParserDirectoryDesc getDirectoryDesc() {
        return directoryDesc;
    }

    public boolean isCTAS() {
        return false;
    }

    /** Retrieve skewed column name for a table. */
    public List<String> getSkewedColumnNames(String alias) {
        // currently, skew column means nothing for flink, so we just return an empty list.
        return Collections.emptyList();
    }

    public HiveParserPTFInvocationSpec getPTFInvocationSpec(HiveParserASTNode node) {
        return ptfNodeToSpec == null ? null : ptfNodeToSpec.get(node);
    }

    public void addPTFNodeToSpec(HiveParserASTNode node, HiveParserPTFInvocationSpec spec) {
        // Must be deterministic order map - see HIVE-8707
        ptfNodeToSpec = ptfNodeToSpec == null ? new LinkedHashMap<>() : ptfNodeToSpec;
        ptfNodeToSpec.put(node, spec);
    }

    public HiveParserWindowingSpec getWindowingSpec(String dest) {
        return destToWindowingSpec.get(dest);
    }

    public void addDestToWindowingSpec(String dest, HiveParserWindowingSpec windowingSpec) {
        destToWindowingSpec.put(dest, windowingSpec);
    }

    public boolean hasWindowingSpec(String dest) {
        return destToWindowingSpec.get(dest) != null;
    }

    public HashMap<String, HiveParserWindowingSpec> getAllWindowingSpecs() {
        return destToWindowingSpec;
    }

    public int incrNumSubQueryPredicates() {
        return ++numSubQueryPredicates;
    }

    public HashMap<String, CatalogView> getViewToTabSchema() {
        return viewAliasToViewSchema;
    }

    public boolean isInsideView() {
        return insideView;
    }

    public void setInsideView(boolean insideView) {
        this.insideView = insideView;
    }

    public Set<String> getAliasInsideView() {
        return aliasInsideView;
    }

    /**
     * returns true, if the query block contains any query, or subquery without a source table. Like
     * select current_user(), select current_database()
     *
     * @return true, if the query block contains any query without a source table
     */
    public boolean containsQueryWithoutSourceTable() {
        for (HiveParserQBExpr qbexpr : aliasToSubq.values()) {
            if (qbexpr.containsQueryWithoutSourceTable()) {
                return true;
            }
        }
        return aliasToTabs.size() == 0 && aliasToSubq.size() == 0;
    }

    public boolean isMaterializedView() {
        return false;
    }

    public Map<String, Tuple2<ResolvedCatalogTable, List<List<String>>>> getValuesTableToData() {
        return valuesTableToData;
    }
}
