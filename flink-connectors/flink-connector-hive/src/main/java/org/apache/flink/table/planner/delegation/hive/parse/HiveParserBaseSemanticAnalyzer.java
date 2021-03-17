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

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.planner.delegation.hive.HiveParserConstants;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserCreateTableDesc.NotNullConstraint;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserCreateTableDesc.PrimaryKey;

import org.antlr.runtime.tree.Tree;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer, and also contains
 * code from SemanticAnalyzer and CalcitePlanner in order to limit file sizes.
 */
public class HiveParserBaseSemanticAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserBaseSemanticAnalyzer.class);

    private HiveParserBaseSemanticAnalyzer() {}

    public static List<FieldSchema> getColumns(ASTNode ast) throws SemanticException {
        return getColumns(ast, true);
    }

    public static List<FieldSchema> getColumns(ASTNode ast, boolean lowerCase)
            throws SemanticException {
        return getColumns(ast, lowerCase, new ArrayList<>(), new ArrayList<>());
    }

    public static String getTypeStringFromAST(ASTNode typeNode) throws SemanticException {
        switch (typeNode.getType()) {
            case HiveASTParser.TOK_LIST:
                return serdeConstants.LIST_TYPE_NAME
                        + "<"
                        + getTypeStringFromAST((ASTNode) typeNode.getChild(0))
                        + ">";
            case HiveASTParser.TOK_MAP:
                return serdeConstants.MAP_TYPE_NAME
                        + "<"
                        + getTypeStringFromAST((ASTNode) typeNode.getChild(0))
                        + ","
                        + getTypeStringFromAST((ASTNode) typeNode.getChild(1))
                        + ">";
            case HiveASTParser.TOK_STRUCT:
                return getStructTypeStringFromAST(typeNode);
            case HiveASTParser.TOK_UNIONTYPE:
                return getUnionTypeStringFromAST(typeNode);
            default:
                return HiveParserDDLSemanticAnalyzer.getTypeName(typeNode);
        }
    }

    private static String getStructTypeStringFromAST(ASTNode typeNode) throws SemanticException {
        String typeStr = serdeConstants.STRUCT_TYPE_NAME + "<";
        typeNode = (ASTNode) typeNode.getChild(0);
        int children = typeNode.getChildCount();
        if (children <= 0) {
            throw new SemanticException("empty struct not allowed.");
        }
        StringBuilder buffer = new StringBuilder(typeStr);
        for (int i = 0; i < children; i++) {
            ASTNode child = (ASTNode) typeNode.getChild(i);
            buffer.append(unescapeIdentifier(child.getChild(0).getText())).append(":");
            buffer.append(getTypeStringFromAST((ASTNode) child.getChild(1)));
            if (i < children - 1) {
                buffer.append(",");
            }
        }

        buffer.append(">");
        return buffer.toString();
    }

    private static String getUnionTypeStringFromAST(ASTNode typeNode) throws SemanticException {
        String typeStr = serdeConstants.UNION_TYPE_NAME + "<";
        typeNode = (ASTNode) typeNode.getChild(0);
        int children = typeNode.getChildCount();
        if (children <= 0) {
            throw new SemanticException("empty union not allowed.");
        }
        StringBuilder buffer = new StringBuilder(typeStr);
        for (int i = 0; i < children; i++) {
            buffer.append(getTypeStringFromAST((ASTNode) typeNode.getChild(i)));
            if (i < children - 1) {
                buffer.append(",");
            }
        }
        buffer.append(">");
        typeStr = buffer.toString();
        return typeStr;
    }

    public static List<FieldSchema> getColumns(
            ASTNode ast,
            boolean lowerCase,
            List<PrimaryKey> primaryKeys,
            List<NotNullConstraint> notNulls)
            throws SemanticException {
        List<FieldSchema> colList = new ArrayList<>();
        int numCh = ast.getChildCount();
        List<PKInfo> pkInfos = new ArrayList<>();
        Map<String, FieldSchema> nametoFS = new HashMap<>();
        Tree parent = ast.getParent();

        for (int i = 0; i < numCh; i++) {
            FieldSchema col = new FieldSchema();
            ASTNode child = (ASTNode) ast.getChild(i);
            if (child.getToken().getType() == HiveASTParser.TOK_PRIMARY_KEY) {
                processPrimaryKeyInfos(child, pkInfos);
            } else if (child.getToken().getType() == HiveASTParser.TOK_FOREIGN_KEY) {
                throw new SemanticException("FOREIGN KEY is not supported.");
            } else {
                Tree grandChild = child.getChild(0);
                if (grandChild != null) {
                    String name = grandChild.getText();
                    if (lowerCase) {
                        name = name.toLowerCase();
                    }
                    checkColumnName(name);
                    // child 0 is the name of the column
                    col.setName(unescapeIdentifier(name));
                    // child 1 is the type of the column
                    ASTNode typeChild = (ASTNode) (child.getChild(1));
                    col.setType(getTypeStringFromAST(typeChild));

                    // child 2 is the optional comment of the column
                    // child 3 is the optional constraint
                    ASTNode constraintChild = null;
                    if (child.getChildCount() == 4) {
                        col.setComment(unescapeSQLString(child.getChild(2).getText()));
                        constraintChild = (ASTNode) child.getChild(3);
                    } else if (child.getChildCount() == 3
                            && ((ASTNode) child.getChild(2)).getToken().getType()
                                    == HiveASTParser.StringLiteral) {
                        col.setComment(unescapeSQLString(child.getChild(2).getText()));
                    } else if (child.getChildCount() == 3) {
                        constraintChild = (ASTNode) child.getChild(2);
                    }
                    if (constraintChild != null) {
                        String[] qualifiedTabName =
                                getQualifiedTableName((ASTNode) parent.getChild(0));
                        switch (constraintChild.getToken().getType()) {
                            case HiveASTParser.TOK_NOT_NULL:
                                notNulls.add(
                                        processNotNull(
                                                constraintChild,
                                                qualifiedTabName[0],
                                                qualifiedTabName[1],
                                                col.getName()));
                                break;
                            default:
                                throw new SemanticException(
                                        "Unsupported constraint node: " + constraintChild);
                        }
                    }
                }
                nametoFS.put(col.getName(), col);
                colList.add(col);
            }
        }
        if (!pkInfos.isEmpty()) {
            processPrimaryKeys((ASTNode) parent, pkInfos, primaryKeys, nametoFS);
        }
        return colList;
    }

    private static NotNullConstraint processNotNull(
            ASTNode node, String dbName, String tblName, String colName) throws SemanticException {
        boolean enable = true;
        boolean validate = false;
        boolean rely = false;
        for (int i = 0; i < node.getChildCount(); i++) {
            ASTNode child = (ASTNode) node.getChild(i);
            switch (child.getToken().getType()) {
                case HiveASTParser.TOK_ENABLE:
                case HiveASTParser.TOK_NOVALIDATE:
                case HiveASTParser.TOK_NORELY:
                    break;
                case HiveASTParser.TOK_DISABLE:
                    enable = false;
                    break;
                case HiveASTParser.TOK_VALIDATE:
                    validate = true;
                    break;
                case HiveASTParser.TOK_RELY:
                    rely = true;
                    break;
                default:
                    throw new SemanticException(
                            "Unexpected node for NOT NULL constraint: " + child);
            }
        }
        return new NotNullConstraint(dbName, tblName, colName, null, enable, validate, rely);
    }

    private static void processPrimaryKeys(
            ASTNode parent,
            List<PKInfo> pkInfos,
            List<PrimaryKey> primaryKeys,
            Map<String, FieldSchema> nametoFS)
            throws SemanticException {
        int cnt = 1;
        String[] qualifiedTabName = getQualifiedTableName((ASTNode) parent.getChild(0));

        for (PKInfo pkInfo : pkInfos) {
            String pk = pkInfo.colName;
            if (nametoFS.containsKey(pk)) {
                PrimaryKey currPrimaryKey =
                        new PrimaryKey(
                                qualifiedTabName[0],
                                qualifiedTabName[1],
                                pk,
                                pkInfo.constraintName,
                                false,
                                false,
                                pkInfo.rely);
                primaryKeys.add(currPrimaryKey);
            } else {
                throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(pk));
            }
        }
    }

    private static void processPrimaryKeyInfos(ASTNode child, List<PKInfo> pkInfos)
            throws SemanticException {
        if (child.getChildCount() < 4) {
            throw new SemanticException("Invalid Primary Key syntax");
        }
        // The ANTLR grammar looks like :
        // 1. KW_CONSTRAINT idfr=identifier KW_PRIMARY KW_KEY pkCols=columnParenthesesList
        //  enableSpec=enableSpecification validateSpec=validateSpecification
        // relySpec=relySpecification
        // -> ^(TOK_PRIMARY_KEY $pkCols $idfr $relySpec $enableSpec $validateSpec)
        // when the user specifies the constraint name (i.e. child.getChildCount() == 5)
        // 2.  KW_PRIMARY KW_KEY columnParenthesesList
        // enableSpec=enableSpecification validateSpec=validateSpecification
        // relySpec=relySpecification
        // -> ^(TOK_PRIMARY_KEY columnParenthesesList $relySpec $enableSpec $validateSpec)
        // when the user does not specify the constraint name (i.e. child.getChildCount() == 4)
        boolean userSpecifiedConstraintName = child.getChildCount() == 5;
        int relyIndex = child.getChildCount() == 5 ? 2 : 1;
        for (int j = 0; j < child.getChild(0).getChildCount(); j++) {
            Tree grandChild = child.getChild(0).getChild(j);
            boolean rely = child.getChild(relyIndex).getType() == HiveASTParser.TOK_VALIDATE;
            boolean enable = child.getChild(relyIndex + 1).getType() == HiveASTParser.TOK_ENABLE;
            boolean validate =
                    child.getChild(relyIndex + 2).getType() == HiveASTParser.TOK_VALIDATE;
            if (enable) {
                throw new SemanticException(
                        "Invalid Primary Key syntax ENABLE feature not supported yet");
            }
            if (validate) {
                throw new SemanticException(
                        "Invalid Primary Key syntax VALIDATE feature not supported yet");
            }
            checkColumnName(grandChild.getText());
            pkInfos.add(
                    new PKInfo(
                            unescapeIdentifier(grandChild.getText().toLowerCase()),
                            (userSpecifiedConstraintName
                                    ? unescapeIdentifier(child.getChild(1).getText().toLowerCase())
                                    : null),
                            rely));
        }
    }

    private static void checkColumnName(String columnName) throws SemanticException {
        if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(columnName.toUpperCase())) {
            throw new SemanticException("Invalid column name " + columnName);
        }
    }

    public static String getDotName(String[] qname) throws SemanticException {
        String genericName = StringUtils.join(qname, ".");
        if (qname.length != 2) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME, genericName);
        }
        return genericName;
    }

    /**
     * Converts parsed key/value properties pairs into a map.
     *
     * @param prop ASTNode parent of the key/value pairs
     * @param mapProp property map which receives the mappings
     */
    public static void readProps(ASTNode prop, Map<String, String> mapProp) {

        for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
            String key = unescapeSQLString(prop.getChild(propChild).getChild(0).getText());
            String value = null;
            if (prop.getChild(propChild).getChild(1) != null) {
                value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
            }
            mapProp.put(key, value);
        }
    }

    public static String[] getQualifiedTableName(ASTNode tabNameNode) throws SemanticException {
        if (tabNameNode.getType() != HiveASTParser.TOK_TABNAME
                || (tabNameNode.getChildCount() != 1 && tabNameNode.getChildCount() != 2)) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME.getMsg(tabNameNode));
        }
        if (tabNameNode.getChildCount() == 2) {
            String dbName = unescapeIdentifier(tabNameNode.getChild(0).getText());
            String tableName = unescapeIdentifier(tabNameNode.getChild(1).getText());
            return new String[] {dbName, tableName};
        }
        String tableName = unescapeIdentifier(tabNameNode.getChild(0).getText());
        return Utilities.getDbTableName(tableName);
    }

    public static Tuple2<String, String> charSetString(String charSetName, String charSetString)
            throws SemanticException {
        try {
            // The character set name starts with a _, so strip that
            charSetName = charSetName.substring(1);
            if (charSetString.charAt(0) == '\'') {
                return Tuple2.of(
                        charSetName,
                        new String(unescapeSQLString(charSetString).getBytes(), charSetName));
            } else {
                assert charSetString.charAt(0) == '0';
                assert charSetString.charAt(1) == 'x';
                charSetString = charSetString.substring(2);

                byte[] bArray = new byte[charSetString.length() / 2];
                int j = 0;
                for (int i = 0; i < charSetString.length(); i += 2) {
                    int val =
                            Character.digit(charSetString.charAt(i), 16) * 16
                                    + Character.digit(charSetString.charAt(i + 1), 16);
                    if (val > 127) {
                        val = val - 256;
                    }
                    bArray[j++] = (byte) val;
                }

                return Tuple2.of(charSetName, new String(bArray, charSetName));
            }
        } catch (UnsupportedEncodingException e) {
            throw new SemanticException(e);
        }
    }

    public static String stripQuotes(String val) {
        return PlanUtils.stripQuotes(val);
    }

    /**
     * Remove the encapsulating "`" pair from the identifier. We allow users to use "`" to escape
     * identifier for table names, column names and aliases, in case that coincide with Hive
     * language keywords.
     */
    public static String unescapeIdentifier(String val) {
        if (val == null) {
            return null;
        }
        if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

    /**
     * Get the unqualified name from a table node. This method works for table names qualified with
     * their schema (e.g., "db.table") and table names without schema qualification. In both cases,
     * it returns the table name without the schema.
     *
     * @param node the table node
     * @return the table name without schema qualification (i.e., if name is "db.table" or "table",
     *     returns "table")
     */
    public static String getUnescapedUnqualifiedTableName(ASTNode node) {
        assert node.getChildCount() <= 2;

        if (node.getChildCount() == 2) {
            node = (ASTNode) node.getChild(1);
        }

        return getUnescapedName(node);
    }

    /**
     * Get dequoted name from a table/column node.
     *
     * @param tableOrColumnNode the table or column node
     * @return for table node, db.tab or tab. for column node column.
     */
    public static String getUnescapedName(ASTNode tableOrColumnNode) {
        return getUnescapedName(tableOrColumnNode, null);
    }

    public static String getUnescapedName(ASTNode tableOrColumnNode, String currentDatabase) {
        int tokenType = tableOrColumnNode.getToken().getType();
        if (tokenType == HiveASTParser.TOK_TABNAME) {
            // table node
            Map.Entry<String, String> dbTablePair = getDbTableNamePair(tableOrColumnNode);
            String dbName = dbTablePair.getKey();
            String tableName = dbTablePair.getValue();
            if (dbName != null) {
                return dbName + "." + tableName;
            }
            if (currentDatabase != null) {
                return currentDatabase + "." + tableName;
            }
            return tableName;
        } else if (tokenType == HiveASTParser.StringLiteral) {
            return unescapeSQLString(tableOrColumnNode.getText());
        }
        // column node
        return unescapeIdentifier(tableOrColumnNode.getText());
    }

    public static Map.Entry<String, String> getDbTableNamePair(ASTNode tableNameNode) {
        assert (tableNameNode.getToken().getType() == HiveASTParser.TOK_TABNAME);
        if (tableNameNode.getChildCount() == 2) {
            String dbName = unescapeIdentifier(tableNameNode.getChild(0).getText());
            String tableName = unescapeIdentifier(tableNameNode.getChild(1).getText());
            return Pair.of(dbName, tableName);
        } else {
            String tableName = unescapeIdentifier(tableNameNode.getChild(0).getText());
            return Pair.of(null, tableName);
        }
    }

    @SuppressWarnings("nls")
    public static String unescapeSQLString(String b) {
        Character enclosure = null;

        // Some of the strings can be passed in as unicode. For example, the
        // delimiter can be passed in as \002 - So, we first check if the
        // string is a unicode number, else go back to the old behavior
        StringBuilder sb = new StringBuilder(b.length());
        for (int i = 0; i < b.length(); i++) {

            char currentChar = b.charAt(i);
            if (enclosure == null) {
                if (currentChar == '\'' || b.charAt(i) == '\"') {
                    enclosure = currentChar;
                }
                // ignore all other chars outside the enclosure
                continue;
            }

            if (enclosure.equals(currentChar)) {
                enclosure = null;
                continue;
            }

            if (currentChar == '\\' && (i + 6 < b.length()) && b.charAt(i + 1) == 'u') {
                int code = 0;
                int base = i + 2;
                for (int j = 0; j < 4; j++) {
                    int digit = Character.digit(b.charAt(j + base), 16);
                    code = (code << 4) + digit;
                }
                sb.append((char) code);
                i += 5;
                continue;
            }

            if (currentChar == '\\' && (i + 4 < b.length())) {
                char i1 = b.charAt(i + 1);
                char i2 = b.charAt(i + 2);
                char i3 = b.charAt(i + 3);
                if ((i1 >= '0' && i1 <= '1')
                        && (i2 >= '0' && i2 <= '7')
                        && (i3 >= '0' && i3 <= '7')) {
                    byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
                    byte[] bValArr = new byte[1];
                    bValArr[0] = bVal;
                    String tmp = new String(bValArr);
                    sb.append(tmp);
                    i += 3;
                    continue;
                }
            }

            if (currentChar == '\\' && (i + 2 < b.length())) {
                char n = b.charAt(i + 1);
                switch (n) {
                    case '0':
                        sb.append("\0");
                        break;
                    case '\'':
                        sb.append("'");
                        break;
                    case '"':
                        sb.append("\"");
                        break;
                    case 'b':
                        sb.append("\b");
                        break;
                    case 'n':
                        sb.append("\n");
                        break;
                    case 'r':
                        sb.append("\r");
                        break;
                    case 't':
                        sb.append("\t");
                        break;
                    case 'Z':
                        sb.append("\u001A");
                        break;
                    case '\\':
                        sb.append("\\");
                        break;
                        // The following 2 lines are exactly what MySQL does TODO: why do we do
                        // this?
                    case '%':
                        sb.append("\\%");
                        break;
                    case '_':
                        sb.append("\\_");
                        break;
                    default:
                        sb.append(n);
                }
                i++;
            } else {
                sb.append(currentChar);
            }
        }
        return sb.toString();
    }

    private static String stripIdentifierQuotes(String val) {
        if ((val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`')) {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

    static List<ASTNode> doPhase1GetDistinctFuncExprs(HashMap<String, ASTNode> aggregationTrees) {
        List<ASTNode> exprs = new ArrayList<>();
        for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
            ASTNode value = entry.getValue();
            if (value.getToken().getType() == HiveASTParser.TOK_FUNCTIONDI) {
                exprs.add(value);
            }
        }
        return exprs;
    }

    static String findSimpleTableName(ASTNode tabref, int aliasIndex) {
        assert tabref.getType() == HiveASTParser.TOK_TABREF;
        ASTNode tableTree = (ASTNode) (tabref.getChild(0));

        String alias;
        if (aliasIndex != 0) {
            alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
        } else {
            alias = getUnescapedUnqualifiedTableName(tableTree);
        }
        return alias;
    }

    static int[] findTabRefIdxs(ASTNode tabref) {
        assert tabref.getType() == HiveASTParser.TOK_TABREF;
        int aliasIndex = 0;
        int propsIndex = -1;
        int tsampleIndex = -1;
        int ssampleIndex = -1;
        for (int index = 1; index < tabref.getChildCount(); index++) {
            ASTNode ct = (ASTNode) tabref.getChild(index);
            if (ct.getToken().getType() == HiveASTParser.TOK_TABLEBUCKETSAMPLE) {
                tsampleIndex = index;
            } else if (ct.getToken().getType() == HiveASTParser.TOK_TABLESPLITSAMPLE) {
                ssampleIndex = index;
            } else if (ct.getToken().getType() == HiveASTParser.TOK_TABLEPROPERTIES) {
                propsIndex = index;
            } else {
                aliasIndex = index;
            }
        }
        return new int[] {aliasIndex, propsIndex, tsampleIndex, ssampleIndex};
    }

    // Take an expression in the values clause and turn it back into a string.  This is far from
    // comprehensive.  At the moment it only supports:
    // * literals (all types)
    // * unary negatives
    // * true/false
    static String unparseExprForValuesClause(ASTNode expr) throws SemanticException {
        switch (expr.getToken().getType()) {
            case HiveASTParser.Number:
                return expr.getText();
            case HiveASTParser.StringLiteral:
                return unescapeSQLString(expr.getText());
            case HiveASTParser.KW_FALSE:
                // UDFToBoolean casts any non-empty string to true, so set this to false
                return "";
            case HiveASTParser.KW_TRUE:
                return "TRUE";
            case HiveASTParser.MINUS:
                return "-" + unparseExprForValuesClause((ASTNode) expr.getChildren().get(0));
            case HiveASTParser.TOK_NULL:
                // Hive's text input will translate this as a null
                return "\\N";
            default:
                throw new SemanticException(
                        "Expression of type " + expr.getText() + " not supported in insert/values");
        }
    }

    public static String getColumnInternalName(int pos) {
        return HiveConf.getColumnInternalName(pos);
    }

    static List<Integer> getGroupingSetsForRollup(int size) {
        List<Integer> groupingSetKeys = new ArrayList<>();
        for (int i = 0; i <= size; i++) {
            groupingSetKeys.add((1 << i) - 1);
        }
        return groupingSetKeys;
    }

    static List<Integer> getGroupingSetsForCube(int size) {
        int count = 1 << size;
        List<Integer> results = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            results.add(i);
        }
        return results;
    }

    private static boolean checkForEmptyGroupingSets(List<Integer> bitmaps, int groupingIdAllSet) {
        boolean ret = true;
        for (int mask : bitmaps) {
            ret &= mask == groupingIdAllSet;
        }
        return ret;
    }

    public static Phase1Ctx initPhase1Ctx() {

        Phase1Ctx ctx1 = new Phase1Ctx();
        ctx1.nextNum = 0;
        ctx1.dest = "reduce";

        return ctx1;
    }

    static void warn(String msg) {
        SessionState.getConsole().printInfo(String.format("Warning: %s", msg));
    }

    // Process the position alias in GROUPBY and ORDERBY
    static void processPositionAlias(ASTNode ast, HiveConf conf) throws SemanticException {
        boolean isBothByPos =
                HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_GROUPBY_ORDERBY_POSITION_ALIAS);
        boolean isGbyByPos =
                isBothByPos
                        || Boolean.parseBoolean(conf.get("hive.groupby.position.alias", "false"));
        boolean isObyByPos =
                isBothByPos
                        || Boolean.parseBoolean(conf.get("hive.orderby.position.alias", "true"));

        Deque<ASTNode> stack = new ArrayDeque<>();
        stack.push(ast);

        while (!stack.isEmpty()) {
            ASTNode next = stack.pop();

            if (next.getChildCount() == 0) {
                continue;
            }

            boolean isAllCol;
            ASTNode selectNode = null;
            ASTNode groupbyNode = null;
            ASTNode orderbyNode = null;

            // get node type
            int childCount = next.getChildCount();
            for (int childPos = 0; childPos < childCount; ++childPos) {
                ASTNode node = (ASTNode) next.getChild(childPos);
                int type = node.getToken().getType();
                if (type == HiveASTParser.TOK_SELECT) {
                    selectNode = node;
                } else if (type == HiveASTParser.TOK_GROUPBY) {
                    groupbyNode = node;
                } else if (type == HiveASTParser.TOK_ORDERBY) {
                    orderbyNode = node;
                }
            }

            if (selectNode != null) {
                int selectExpCnt = selectNode.getChildCount();

                // replace each of the position alias in GROUPBY with the actual column name
                if (groupbyNode != null) {
                    for (int childPos = 0; childPos < groupbyNode.getChildCount(); ++childPos) {
                        ASTNode node = (ASTNode) groupbyNode.getChild(childPos);
                        if (node.getToken().getType() == HiveASTParser.Number) {
                            if (isGbyByPos) {
                                int pos = Integer.parseInt(node.getText());
                                if (pos > 0 && pos <= selectExpCnt) {
                                    groupbyNode.setChild(
                                            childPos, selectNode.getChild(pos - 1).getChild(0));
                                } else {
                                    throw new SemanticException(
                                            ErrorMsg.INVALID_POSITION_ALIAS_IN_GROUPBY.getMsg(
                                                    "Position alias: "
                                                            + pos
                                                            + " does not exist\n"
                                                            + "The Select List is indexed from 1 to "
                                                            + selectExpCnt));
                                }
                            } else {
                                warn(
                                        "Using constant number  "
                                                + node.getText()
                                                + " in group by. If you try to use position alias when hive.groupby.position.alias is false, the position alias will be ignored.");
                            }
                        }
                    }
                }

                // replace each of the position alias in ORDERBY with the actual column name
                if (orderbyNode != null) {
                    isAllCol = false;
                    for (int childPos = 0; childPos < selectNode.getChildCount(); ++childPos) {
                        ASTNode node = (ASTNode) selectNode.getChild(childPos).getChild(0);
                        if (node != null
                                && node.getToken().getType() == HiveASTParser.TOK_ALLCOLREF) {
                            isAllCol = true;
                        }
                    }
                    for (int childPos = 0; childPos < orderbyNode.getChildCount(); ++childPos) {
                        ASTNode colNode = (ASTNode) orderbyNode.getChild(childPos).getChild(0);
                        ASTNode node = (ASTNode) colNode.getChild(0);
                        if (node != null && node.getToken().getType() == HiveASTParser.Number) {
                            if (isObyByPos) {
                                if (!isAllCol) {
                                    int pos = Integer.parseInt(node.getText());
                                    if (pos > 0 && pos <= selectExpCnt) {
                                        colNode.setChild(
                                                0, selectNode.getChild(pos - 1).getChild(0));
                                    } else {
                                        throw new SemanticException(
                                                ErrorMsg.INVALID_POSITION_ALIAS_IN_ORDERBY.getMsg(
                                                        "Position alias: "
                                                                + pos
                                                                + " does not exist\n"
                                                                + "The Select List is indexed from 1 to "
                                                                + selectExpCnt));
                                    }
                                } else {
                                    throw new SemanticException(
                                            ErrorMsg.NO_SUPPORTED_ORDERBY_ALLCOLREF_POS.getMsg());
                                }
                            } else { // if not using position alias and it is a number.
                                warn(
                                        "Using constant number "
                                                + node.getText()
                                                + " in order by. If you try to use position alias when hive.orderby.position.alias is false, the position alias will be ignored.");
                            }
                        }
                    }
                }
            }

            for (int i = next.getChildren().size() - 1; i >= 0; i--) {
                stack.push((ASTNode) next.getChildren().get(i));
            }
        }
    }

    static PTFInvocationSpec.PartitionSpec processPartitionSpec(ASTNode node) {
        PTFInvocationSpec.PartitionSpec pSpec = new PTFInvocationSpec.PartitionSpec();
        int exprCnt = node.getChildCount();
        for (int i = 0; i < exprCnt; i++) {
            PTFInvocationSpec.PartitionExpression exprSpec =
                    new PTFInvocationSpec.PartitionExpression();
            exprSpec.setExpression((ASTNode) node.getChild(i));
            pSpec.addExpression(exprSpec);
        }
        return pSpec;
    }

    static boolean containsLeadLagUDF(ASTNode expressionTree) {
        int exprTokenType = expressionTree.getToken().getType();
        if (exprTokenType == HiveASTParser.TOK_FUNCTION) {
            assert (expressionTree.getChildCount() != 0);
            if (expressionTree.getChild(0).getType() == HiveASTParser.Identifier) {
                String functionName = unescapeIdentifier(expressionTree.getChild(0).getText());
                functionName = functionName.toLowerCase();
                if (FunctionRegistry.LAG_FUNC_NAME.equals(functionName)
                        || FunctionRegistry.LEAD_FUNC_NAME.equals(functionName)) {
                    return true;
                }
            }
        }
        for (int i = 0; i < expressionTree.getChildCount(); i++) {
            if (containsLeadLagUDF((ASTNode) expressionTree.getChild(i))) {
                return true;
            }
        }
        return false;
    }

    static TableType obtainTableType(Table tabMetaData) {
        if (tabMetaData.getStorageHandler() != null
                && tabMetaData
                        .getStorageHandler()
                        .toString()
                        .equals(HiveParserConstants.DRUID_HIVE_STORAGE_HANDLER_ID)) {
            return TableType.DRUID;
        }
        return TableType.NATIVE;
    }

    /* This method returns the flip big-endian representation of value */
    static ImmutableBitSet convert(int value, int length) {
        BitSet bits = new BitSet();
        for (int index = length - 1; index >= 0; index--) {
            if (value % 2 != 0) {
                bits.set(index);
            }
            value = value >>> 1;
        }
        // We flip the bits because Calcite considers that '1'
        // means that the column participates in the GroupBy
        // and '0' does not, as opposed to grouping_id.
        bits.flip(0, length);
        return ImmutableBitSet.fromBitSet(bits);
    }

    static boolean topLevelConjunctCheck(
            ASTNode searchCond, ObjectPair<Boolean, Integer> subqInfo) {
        if (searchCond.getType() == HiveASTParser.KW_OR) {
            subqInfo.setFirst(Boolean.TRUE);
            if (subqInfo.getSecond() > 1) {
                return false;
            }
        }
        if (searchCond.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
            subqInfo.setSecond(subqInfo.getSecond() + 1);
            return subqInfo.getSecond() <= 1 || !subqInfo.getFirst();
        }
        for (int i = 0; i < searchCond.getChildCount(); i++) {
            boolean validSubQuery =
                    topLevelConjunctCheck((ASTNode) searchCond.getChild(i), subqInfo);
            if (!validSubQuery) {
                return false;
            }
        }
        return true;
    }

    static int getWindowSpecIndx(ASTNode wndAST) {
        int wi = wndAST.getChildCount() - 1;
        if (wi <= 0 || (wndAST.getChild(wi).getType() != HiveASTParser.TOK_WINDOWSPEC)) {
            wi = -1;
        }
        return wi;
    }

    private static void errorPartSpec(Map<String, String> partSpec, List<FieldSchema> parts)
            throws SemanticException {
        StringBuilder sb = new StringBuilder("Partition columns in the table schema are: (");
        for (FieldSchema fs : parts) {
            sb.append(fs.getName()).append(", ");
        }
        sb.setLength(sb.length() - 2); // remove the last ", "
        sb.append("), while the partitions specified in the query are: (");

        Iterator<String> itrPsKeys = partSpec.keySet().iterator();
        while (itrPsKeys.hasNext()) {
            sb.append(itrPsKeys.next()).append(", ");
        }
        sb.setLength(sb.length() - 2); // remove the last ", "
        sb.append(").");
        throw new SemanticException(ErrorMsg.PARTSPEC_DIFFER_FROM_SCHEMA.getMsg(sb.toString()));
    }

    /** Counterpart of hive's BaseSemanticAnalyzer.AnalyzeRewriteContext. */
    public static class AnalyzeRewriteContext {

        private String tableName;
        private List<String> colName;
        private List<String> colType;
        private boolean tblLvl;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public List<String> getColName() {
            return colName;
        }

        public void setColName(List<String> colName) {
            this.colName = colName;
        }

        public boolean isTblLvl() {
            return tblLvl;
        }

        public void setTblLvl(boolean isTblLvl) {
            this.tblLvl = isTblLvl;
        }

        public List<String> getColType() {
            return colType;
        }

        public void setColType(List<String> colType) {
            this.colType = colType;
        }
    }

    /** Counterpart of hive's BaseSemanticAnalyzer.PKInfo. */
    private static class PKInfo {
        public String colName;
        public String constraintName;
        public boolean rely;

        public PKInfo(String colName, String constraintName, boolean rely) {
            this.colName = colName;
            this.constraintName = constraintName;
            this.rely = rely;
        }
    }

    /** Counterpart of hive's SemanticAnalyzer.Phase1Ctx. */
    static class Phase1Ctx {
        String dest;
        int nextNum;
    }

    /** Counterpart of hive's SemanticAnalyzer.GenericUDAFInfo. */
    public static class GenericUDAFInfo {
        public ArrayList<ExprNodeDesc> convertedParameters;
        public GenericUDAFEvaluator genericUDAFEvaluator;
        public TypeInfo returnType;
    }

    /** Counterpart of hive's CalcitePlanner.TableType. */
    public enum TableType {
        DRUID,
        NATIVE
    }

    /** Counterpart of hive's CalcitePlanner.AggInfo. */
    static class AggInfo {
        private final List<ExprNodeDesc> aggParams;
        private final TypeInfo returnType;
        private final String udfName;
        private final boolean distinct;
        private final boolean isAllColumns;
        private final String alias;

        AggInfo(
                List<ExprNodeDesc> aggParams,
                TypeInfo returnType,
                String udfName,
                boolean isDistinct,
                boolean isAllColumns,
                String alias) {
            this.aggParams = aggParams;
            this.returnType = returnType;
            this.udfName = udfName;
            distinct = isDistinct;
            this.isAllColumns = isAllColumns;
            this.alias = alias;
        }

        public List<ExprNodeDesc> getAggParams() {
            return aggParams;
        }

        public TypeInfo getReturnType() {
            return returnType;
        }

        public String getUdfName() {
            return udfName;
        }

        public boolean isDistinct() {
            return distinct;
        }

        public boolean isAllColumns() {
            return isAllColumns;
        }

        public String getAlias() {
            return alias;
        }
    }

    /** Counterpart of hive's BaseSemanticAnalyzer.RowFormatParams. */
    public static class HiveParserRowFormatParams {
        String fieldDelim = null;
        String fieldEscape = null;
        String collItemDelim = null;
        String mapKeyDelim = null;
        String lineDelim = null;
        String nullFormat = null;

        public String getFieldDelim() {
            return fieldDelim;
        }

        public String getFieldEscape() {
            return fieldEscape;
        }

        public String getCollItemDelim() {
            return collItemDelim;
        }

        public String getMapKeyDelim() {
            return mapKeyDelim;
        }

        public String getLineDelim() {
            return lineDelim;
        }

        public String getNullFormat() {
            return nullFormat;
        }

        protected void analyzeRowFormat(ASTNode child) throws SemanticException {
            child = (ASTNode) child.getChild(0);
            int numChildRowFormat = child.getChildCount();
            for (int numC = 0; numC < numChildRowFormat; numC++) {
                ASTNode rowChild = (ASTNode) child.getChild(numC);
                switch (rowChild.getToken().getType()) {
                    case HiveASTParser.TOK_TABLEROWFORMATFIELD:
                        fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
                        if (rowChild.getChildCount() >= 2) {
                            fieldEscape = unescapeSQLString(rowChild.getChild(1).getText());
                        }
                        break;
                    case HiveASTParser.TOK_TABLEROWFORMATCOLLITEMS:
                        collItemDelim = unescapeSQLString(rowChild.getChild(0).getText());
                        break;
                    case HiveASTParser.TOK_TABLEROWFORMATMAPKEYS:
                        mapKeyDelim = unescapeSQLString(rowChild.getChild(0).getText());
                        break;
                    case HiveASTParser.TOK_TABLEROWFORMATLINES:
                        lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
                        if (!lineDelim.equals("\n") && !lineDelim.equals("10")) {
                            throw new SemanticException(
                                    HiveParserUtils.generateErrorMessage(
                                            rowChild,
                                            ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
                        }
                        break;
                    case HiveASTParser.TOK_TABLEROWFORMATNULL:
                        nullFormat = unescapeSQLString(rowChild.getChild(0).getText());
                        break;
                    default:
                        throw new AssertionError("Unkown Token: " + rowChild);
                }
            }
        }
    }
}
