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

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private final HashMap<String, String> aliasToTabs;
    private final HashMap<String, HiveParserQBExpr> aliasToSubq;
    private final HashMap<String, Table> viewAliasToViewSchema;
    private final HashMap<String, Map<String, String>> aliasToProps;
    private final List<String> aliases;
    private final HiveParserQBParseInfo qbp;
    private final QBMetaData qbm;
    private final String id;
    private boolean isQuery;
    private boolean insideView;
    private Set<String> aliasInsideView;
    private final Map<String, List<List<String>>> valuesTableToData = new HashMap<>();

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
        aliasToTabs = new LinkedHashMap<>();
        aliasToSubq = new LinkedHashMap<>();
        viewAliasToViewSchema = new LinkedHashMap<>();
        aliasToProps = new LinkedHashMap<>();
        aliases = new ArrayList<>();
        if (alias != null) {
            alias = alias.toLowerCase();
        }
        qbp = new HiveParserQBParseInfo(alias, isSubQ);
        qbm = new QBMetaData();
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

    public QBMetaData getMetaData() {
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

    public void setTabAlias(String alias, String tabName) {
        aliasToTabs.put(alias.toLowerCase(), tabName);
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

    public void rewriteViewToSubq(
            String alias, String viewName, HiveParserQBExpr qbexpr, Table tab) {
        alias = alias.toLowerCase();
        String tableName = aliasToTabs.remove(alias);
        assert (viewName.equals(tableName));
        aliasToSubq.put(alias, qbexpr);
        if (tab != null) {
            viewAliasToViewSchema.put(alias, tab);
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
        return null;
    }

    public void setDirectoryDesc(CreateTableDesc directoryDesc) {}

    public boolean isCTAS() {
        return false;
    }

    /** Retrieve skewed column name for a table. */
    public List<String> getSkewedColumnNames(String alias) {
        List<String> skewedColNames = null;
        if (null != qbm && null != qbm.getAliasToTable() && qbm.getAliasToTable().size() > 0) {
            Table tbl = getMetaData().getTableForAlias(alias);
            skewedColNames = tbl.getSkewedColNames();
        }
        return skewedColNames;
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

    public HashMap<String, Table> getViewToTabSchema() {
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

    public Map<String, List<List<String>>> getValuesTableToData() {
        return valuesTableToData;
    }
}
