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
import org.apache.flink.table.planner.delegation.hive.HiveParserConstants;
import org.apache.flink.table.planner.delegation.hive.HiveParserRexNodeConverter;
import org.apache.flink.table.planner.delegation.hive.HiveParserTypeCheckProcFactory;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.NullOrder;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.OrderExpression;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.OrderSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitionExpression;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitionSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitioningSpec;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserDDLSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserErrorMsg;
import org.apache.flink.util.Preconditions;

import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.removeASTChild;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer, and also contains
 * code from SemanticAnalyzer and CalcitePlanner in order to limit file sizes.
 */
public class HiveParserBaseSemanticAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserBaseSemanticAnalyzer.class);

    private HiveParserBaseSemanticAnalyzer() {}

    public static List<FieldSchema> getColumns(HiveParserASTNode ast) throws SemanticException {
        return getColumns(ast, true);
    }

    public static List<FieldSchema> getColumns(HiveParserASTNode ast, boolean lowerCase)
            throws SemanticException {
        return getColumns(ast, lowerCase, new ArrayList<>(), new ArrayList<>());
    }

    public static String getTypeStringFromAST(HiveParserASTNode typeNode) throws SemanticException {
        switch (typeNode.getType()) {
            case HiveASTParser.TOK_LIST:
                return serdeConstants.LIST_TYPE_NAME
                        + "<"
                        + getTypeStringFromAST((HiveParserASTNode) typeNode.getChild(0))
                        + ">";
            case HiveASTParser.TOK_MAP:
                return serdeConstants.MAP_TYPE_NAME
                        + "<"
                        + getTypeStringFromAST((HiveParserASTNode) typeNode.getChild(0))
                        + ","
                        + getTypeStringFromAST((HiveParserASTNode) typeNode.getChild(1))
                        + ">";
            case HiveASTParser.TOK_STRUCT:
                return getStructTypeStringFromAST(typeNode);
            case HiveASTParser.TOK_UNIONTYPE:
                return getUnionTypeStringFromAST(typeNode);
            default:
                return HiveParserDDLSemanticAnalyzer.getTypeName(typeNode);
        }
    }

    private static String getStructTypeStringFromAST(HiveParserASTNode typeNode)
            throws SemanticException {
        String typeStr = serdeConstants.STRUCT_TYPE_NAME + "<";
        typeNode = (HiveParserASTNode) typeNode.getChild(0);
        int children = typeNode.getChildCount();
        if (children <= 0) {
            throw new SemanticException("empty struct not allowed.");
        }
        StringBuilder buffer = new StringBuilder(typeStr);
        for (int i = 0; i < children; i++) {
            HiveParserASTNode child = (HiveParserASTNode) typeNode.getChild(i);
            buffer.append(unescapeIdentifier(child.getChild(0).getText())).append(":");
            buffer.append(getTypeStringFromAST((HiveParserASTNode) child.getChild(1)));
            if (i < children - 1) {
                buffer.append(",");
            }
        }

        buffer.append(">");
        return buffer.toString();
    }

    private static String getUnionTypeStringFromAST(HiveParserASTNode typeNode)
            throws SemanticException {
        String typeStr = serdeConstants.UNION_TYPE_NAME + "<";
        typeNode = (HiveParserASTNode) typeNode.getChild(0);
        int children = typeNode.getChildCount();
        if (children <= 0) {
            throw new SemanticException("empty union not allowed.");
        }
        StringBuilder buffer = new StringBuilder(typeStr);
        for (int i = 0; i < children; i++) {
            buffer.append(getTypeStringFromAST((HiveParserASTNode) typeNode.getChild(i)));
            if (i < children - 1) {
                buffer.append(",");
            }
        }
        buffer.append(">");
        typeStr = buffer.toString();
        return typeStr;
    }

    public static List<FieldSchema> getColumns(
            HiveParserASTNode ast,
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
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(i);
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
                    HiveParserASTNode typeChild = (HiveParserASTNode) (child.getChild(1));
                    col.setType(getTypeStringFromAST(typeChild));

                    // child 2 is the optional comment of the column
                    // child 3 is the optional constraint
                    HiveParserASTNode constraintChild = null;
                    if (child.getChildCount() == 4) {
                        col.setComment(unescapeSQLString(child.getChild(2).getText()));
                        constraintChild = (HiveParserASTNode) child.getChild(3);
                    } else if (child.getChildCount() == 3
                            && ((HiveParserASTNode) child.getChild(2)).getToken().getType()
                                    == HiveASTParser.StringLiteral) {
                        col.setComment(unescapeSQLString(child.getChild(2).getText()));
                    } else if (child.getChildCount() == 3) {
                        constraintChild = (HiveParserASTNode) child.getChild(2);
                    }
                    if (constraintChild != null) {
                        String[] qualifiedTabName =
                                getQualifiedTableName((HiveParserASTNode) parent.getChild(0));
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
            processPrimaryKeys((HiveParserASTNode) parent, pkInfos, primaryKeys, nametoFS);
        }
        return colList;
    }

    private static NotNullConstraint processNotNull(
            HiveParserASTNode nnNode, String dbName, String tblName, String colName)
            throws SemanticException {
        boolean enable = true;
        boolean validate = false;
        boolean rely = false;
        for (int i = 0; i < nnNode.getChildCount(); i++) {
            HiveParserASTNode child = (HiveParserASTNode) nnNode.getChild(i);
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
            HiveParserASTNode parent,
            List<PKInfo> pkInfos,
            List<PrimaryKey> primaryKeys,
            Map<String, FieldSchema> nametoFS)
            throws SemanticException {
        int cnt = 1;
        String[] qualifiedTabName = getQualifiedTableName((HiveParserASTNode) parent.getChild(0));

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

    private static void processPrimaryKeyInfos(HiveParserASTNode pkNode, List<PKInfo> pkInfos)
            throws SemanticException {
        String userSpecifiedName = null;
        boolean enable = true;
        boolean validate = false;
        boolean rely = false;
        for (int i = 0; i < pkNode.getChildCount(); i++) {
            HiveParserASTNode child = (HiveParserASTNode) pkNode.getChild(i);
            switch (child.getType()) {
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
                case HiveASTParser.TOK_CONSTRAINT_NAME:
                    userSpecifiedName =
                            unescapeIdentifier(child.getChild(0).getText().toLowerCase());
                    break;
                case HiveASTParser.TOK_TABCOLNAME:
                    for (int j = 0; j < child.getChildCount(); j++) {
                        String colName = child.getChild(j).getText();
                        checkColumnName(colName);
                        pkInfos.add(new PKInfo(unescapeIdentifier(colName.toLowerCase())));
                    }
                    break;
                default:
                    throw new SemanticException(
                            "Unexpected node for PRIMARY KEY constraint: " + child);
            }
        }
        if (enable) {
            throw new SemanticException(
                    "Invalid Primary Key syntax ENABLE feature not supported yet");
        }
        if (validate) {
            throw new SemanticException(
                    "Invalid Primary Key syntax VALIDATE feature not supported yet");
        }
        if (pkInfos.isEmpty()) {
            throw new SemanticException("No column specified as the primary key");
        }
        for (PKInfo pkInfo : pkInfos) {
            pkInfo.constraintName = userSpecifiedName;
            pkInfo.rely = rely;
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
     * @param prop HiveParserASTNode parent of the key/value pairs
     * @param mapProp property map which receives the mappings
     */
    public static void readProps(HiveParserASTNode prop, Map<String, String> mapProp) {

        for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
            String key = unescapeSQLString(prop.getChild(propChild).getChild(0).getText());
            String value = null;
            if (prop.getChild(propChild).getChild(1) != null) {
                value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
            }
            mapProp.put(key, value);
        }
    }

    public static String[] getQualifiedTableName(HiveParserASTNode tabNameNode)
            throws SemanticException {
        if (tabNameNode.getType() != HiveASTParser.TOK_TABNAME
                || (tabNameNode.getChildCount() != 1 && tabNameNode.getChildCount() != 2)) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(ErrorMsg.INVALID_TABLE_NAME, tabNameNode));
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
    public static String getUnescapedUnqualifiedTableName(HiveParserASTNode node) {
        assert node.getChildCount() <= 2;

        if (node.getChildCount() == 2) {
            node = (HiveParserASTNode) node.getChild(1);
        }

        return getUnescapedName(node);
    }

    /**
     * Get dequoted name from a table/column node.
     *
     * @param tableOrColumnNode the table or column node
     * @return for table node, db.tab or tab. for column node column.
     */
    public static String getUnescapedName(HiveParserASTNode tableOrColumnNode) {
        return getUnescapedName(tableOrColumnNode, null);
    }

    public static String getUnescapedName(
            HiveParserASTNode tableOrColumnNode, String currentDatabase) {
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

    public static Map.Entry<String, String> getDbTableNamePair(HiveParserASTNode tableNameNode) {
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

    public static void validatePartSpec(
            Table tbl,
            Map<String, String> partSpec,
            HiveParserASTNode astNode,
            HiveConf conf,
            boolean shouldBeFull,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {
        tbl.validatePartColumnNames(partSpec, shouldBeFull);
        validatePartColumnType(tbl, partSpec, astNode, conf, frameworkConfig, cluster);
    }

    private static boolean getPartExprNodeDesc(
            HiveParserASTNode astNode,
            HiveConf conf,
            Map<HiveParserASTNode, ExprNodeDesc> astExprNodeMap,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {

        if (astNode == null) {
            return true;
        } else if ((astNode.getChildren() == null) || (astNode.getChildren().size() == 0)) {
            return astNode.getType() != HiveASTParser.TOK_PARTVAL;
        }

        HiveParserTypeCheckCtx typeCheckCtx =
                new HiveParserTypeCheckCtx(null, frameworkConfig, cluster);
        String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME);
        boolean result = true;
        for (Node childNode : astNode.getChildren()) {
            HiveParserASTNode childASTNode = (HiveParserASTNode) childNode;

            if (childASTNode.getType() != HiveASTParser.TOK_PARTVAL) {
                result =
                        getPartExprNodeDesc(
                                        childASTNode,
                                        conf,
                                        astExprNodeMap,
                                        frameworkConfig,
                                        cluster)
                                && result;
            } else {
                boolean isDynamicPart = childASTNode.getChildren().size() <= 1;
                result = !isDynamicPart && result;
                if (!isDynamicPart) {
                    HiveParserASTNode partVal =
                            (HiveParserASTNode) childASTNode.getChildren().get(1);
                    if (!defaultPartitionName.equalsIgnoreCase(
                            unescapeSQLString(partVal.getText()))) {
                        astExprNodeMap.put(
                                (HiveParserASTNode) childASTNode.getChildren().get(0),
                                HiveParserTypeCheckProcFactory.genExprNode(partVal, typeCheckCtx)
                                        .get(partVal));
                    }
                }
            }
        }
        return result;
    }

    private static String stripIdentifierQuotes(String val) {
        if ((val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`')) {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

    static List<HiveParserASTNode> doPhase1GetDistinctFuncExprs(
            HashMap<String, HiveParserASTNode> aggregationTrees) {
        List<HiveParserASTNode> exprs = new ArrayList<>();
        for (Map.Entry<String, HiveParserASTNode> entry : aggregationTrees.entrySet()) {
            HiveParserASTNode value = entry.getValue();
            if (value.getToken().getType() == HiveASTParser.TOK_FUNCTIONDI) {
                exprs.add(value);
            }
        }
        return exprs;
    }

    static String findSimpleTableName(HiveParserASTNode tabref, int aliasIndex) {
        assert tabref.getType() == HiveASTParser.TOK_TABREF;
        HiveParserASTNode tableTree = (HiveParserASTNode) (tabref.getChild(0));

        String alias;
        if (aliasIndex != 0) {
            alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
        } else {
            alias = getUnescapedUnqualifiedTableName(tableTree);
        }
        return alias;
    }

    static int[] findTabRefIdxs(HiveParserASTNode tabref) {
        assert tabref.getType() == HiveASTParser.TOK_TABREF;
        int aliasIndex = 0;
        int propsIndex = -1;
        int tsampleIndex = -1;
        int ssampleIndex = -1;
        for (int index = 1; index < tabref.getChildCount(); index++) {
            HiveParserASTNode ct = (HiveParserASTNode) tabref.getChild(index);
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
    static String unparseExprForValuesClause(HiveParserASTNode expr) throws SemanticException {
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
                return "-"
                        + unparseExprForValuesClause((HiveParserASTNode) expr.getChildren().get(0));
            case HiveASTParser.TOK_NULL:
                return null;
            default:
                throw new SemanticException(
                        "Expression of type " + expr.getText() + " not supported in insert/values");
        }
    }

    public static String getColumnInternalName(int pos) {
        return HiveConf.getColumnInternalName(pos);
    }

    public static List<Integer> getGroupingSetsForRollup(int size) {
        List<Integer> groupingSetKeys = new ArrayList<>();
        for (int i = 0; i <= size; i++) {
            groupingSetKeys.add((1 << i) - 1);
        }
        return groupingSetKeys;
    }

    public static List<Integer> getGroupingSetsForCube(int size) {
        int count = 1 << size;
        List<Integer> results = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            results.add(i);
        }
        return results;
    }

    public static List<Integer> getGroupingSets(
            List<HiveParserASTNode> groupByExpr, HiveParserQBParseInfo parseInfo, String dest)
            throws SemanticException {
        Map<String, Integer> exprPos = new HashMap<>();
        for (int i = 0; i < groupByExpr.size(); ++i) {
            HiveParserASTNode node = groupByExpr.get(i);
            exprPos.put(node.toStringTree(), i);
        }

        HiveParserASTNode root = parseInfo.getGroupByForClause(dest);
        List<Integer> result = new ArrayList<>(root == null ? 0 : root.getChildCount());
        if (root != null) {
            for (int i = 0; i < root.getChildCount(); ++i) {
                HiveParserASTNode child = (HiveParserASTNode) root.getChild(i);
                if (child.getType() != HiveASTParser.TOK_GROUPING_SETS_EXPRESSION) {
                    continue;
                }
                int bitmap = com.google.common.math.IntMath.pow(2, groupByExpr.size()) - 1;
                for (int j = 0; j < child.getChildCount(); ++j) {
                    String treeAsString = child.getChild(j).toStringTree();
                    Integer pos = exprPos.get(treeAsString);
                    if (pos == null) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        (HiveParserASTNode) child.getChild(j),
                                        ErrorMsg.HIVE_GROUPING_SETS_EXPR_NOT_IN_GROUPBY
                                                .getErrorCodedMsg()));
                    }
                    bitmap = HiveParserUtils.unsetBit(bitmap, groupByExpr.size() - pos - 1);
                }
                result.add(bitmap);
            }
        }
        if (checkForEmptyGroupingSets(
                result, com.google.common.math.IntMath.pow(2, groupByExpr.size()) - 1)) {
            throw new SemanticException("Empty grouping sets not allowed");
        }
        return result;
    }

    private static boolean checkForEmptyGroupingSets(List<Integer> bitmaps, int groupingIdAllSet) {
        boolean ret = true;
        for (int mask : bitmaps) {
            ret &= mask == groupingIdAllSet;
        }
        return ret;
    }

    // This function is a wrapper of parseInfo.getGroupByForClause which automatically translates
    // SELECT DISTINCT a,b,c to SELECT a,b,c GROUP BY a,b,c.
    public static List<HiveParserASTNode> getGroupByForClause(
            HiveParserQBParseInfo parseInfo, String dest) {
        if (parseInfo.getSelForClause(dest).getToken().getType() == HiveASTParser.TOK_SELECTDI) {
            HiveParserASTNode selectExprs = parseInfo.getSelForClause(dest);
            List<HiveParserASTNode> result =
                    new ArrayList<>(selectExprs == null ? 0 : selectExprs.getChildCount());
            if (selectExprs != null) {
                for (int i = 0; i < selectExprs.getChildCount(); ++i) {
                    if (((HiveParserASTNode) selectExprs.getChild(i)).getToken().getType()
                            == HiveASTParser.QUERY_HINT) {
                        continue;
                    }
                    // table.column AS alias
                    HiveParserASTNode grpbyExpr =
                            (HiveParserASTNode) selectExprs.getChild(i).getChild(0);
                    result.add(grpbyExpr);
                }
            }
            return result;
        } else {
            HiveParserASTNode grpByExprs = parseInfo.getGroupByForClause(dest);
            List<HiveParserASTNode> result =
                    new ArrayList<>(grpByExprs == null ? 0 : grpByExprs.getChildCount());
            if (grpByExprs != null) {
                for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
                    HiveParserASTNode grpbyExpr = (HiveParserASTNode) grpByExprs.getChild(i);
                    if (grpbyExpr.getType() != HiveASTParser.TOK_GROUPING_SETS_EXPRESSION) {
                        result.add(grpbyExpr);
                    }
                }
            }
            return result;
        }
    }

    static String getAliasId(String alias, HiveParserQB qb) {
        return (qb.getId() == null ? alias : qb.getId() + ":" + alias).toLowerCase();
    }

    public static RexWindowBound getBound(
            HiveParserWindowingSpec.BoundarySpec spec, RelOptCluster cluster) {
        RexWindowBound res = null;

        if (spec != null) {
            SqlParserPos dummyPos = new SqlParserPos(1, 1);
            SqlNode amt =
                    spec.getAmt() == 0
                                    || spec.getAmt()
                                            == HiveParserWindowingSpec.BoundarySpec.UNBOUNDED_AMOUNT
                            ? null
                            : SqlLiteral.createExactNumeric(
                                    String.valueOf(spec.getAmt()), new SqlParserPos(2, 2));
            RexNode amtLiteral =
                    amt == null
                            ? null
                            : cluster.getRexBuilder()
                                    .makeLiteral(
                                            spec.getAmt(),
                                            cluster.getTypeFactory()
                                                    .createSqlType(SqlTypeName.INTEGER),
                                            true);

            switch (spec.getDirection()) {
                case PRECEDING:
                    if (amt == null) {
                        res =
                                RexWindowBound.create(
                                        SqlWindow.createUnboundedPreceding(dummyPos), null);
                    } else {
                        SqlCall call = (SqlCall) SqlWindow.createPreceding(amt, dummyPos);
                        res =
                                RexWindowBound.create(
                                        call,
                                        cluster.getRexBuilder()
                                                .makeCall(call.getOperator(), amtLiteral));
                    }
                    break;

                case CURRENT:
                    res = RexWindowBound.create(SqlWindow.createCurrentRow(dummyPos), null);
                    break;

                case FOLLOWING:
                    if (amt == null) {
                        res =
                                RexWindowBound.create(
                                        SqlWindow.createUnboundedFollowing(dummyPos), null);
                    } else {
                        SqlCall call = (SqlCall) SqlWindow.createFollowing(amt, dummyPos);
                        res =
                                RexWindowBound.create(
                                        call,
                                        cluster.getRexBuilder()
                                                .makeCall(call.getOperator(), amtLiteral));
                    }
                    break;
            }
        }
        return res;
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

    static void handleQueryWindowClauses(
            HiveParserQB qb, HiveParserBaseSemanticAnalyzer.Phase1Ctx ctx1, HiveParserASTNode node)
            throws SemanticException {
        HiveParserWindowingSpec spec = qb.getWindowingSpec(ctx1.dest);
        for (int i = 0; i < node.getChildCount(); i++) {
            processQueryWindowClause(spec, (HiveParserASTNode) node.getChild(i));
        }
    }

    // Process the position alias in GROUPBY and ORDERBY
    public static void processPositionAlias(HiveParserASTNode ast, HiveConf conf)
            throws SemanticException {
        boolean isBothByPos =
                HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_GROUPBY_ORDERBY_POSITION_ALIAS);
        boolean isGbyByPos =
                isBothByPos
                        || Boolean.parseBoolean(conf.get("hive.groupby.position.alias", "false"));
        boolean isObyByPos =
                isBothByPos
                        || Boolean.parseBoolean(conf.get("hive.orderby.position.alias", "true"));

        Deque<HiveParserASTNode> stack = new ArrayDeque<>();
        stack.push(ast);

        while (!stack.isEmpty()) {
            HiveParserASTNode next = stack.pop();

            if (next.getChildCount() == 0) {
                continue;
            }

            boolean isAllCol;
            HiveParserASTNode selectNode = null;
            HiveParserASTNode groupbyNode = null;
            HiveParserASTNode orderbyNode = null;

            // get node type
            int childCount = next.getChildCount();
            for (int childPos = 0; childPos < childCount; ++childPos) {
                HiveParserASTNode node = (HiveParserASTNode) next.getChild(childPos);
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
                        HiveParserASTNode node = (HiveParserASTNode) groupbyNode.getChild(childPos);
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
                        HiveParserASTNode node =
                                (HiveParserASTNode) selectNode.getChild(childPos).getChild(0);
                        if (node != null
                                && node.getToken().getType() == HiveASTParser.TOK_ALLCOLREF) {
                            isAllCol = true;
                        }
                    }
                    for (int childPos = 0; childPos < orderbyNode.getChildCount(); ++childPos) {
                        HiveParserASTNode colNode =
                                (HiveParserASTNode) orderbyNode.getChild(childPos).getChild(0);
                        HiveParserASTNode node = (HiveParserASTNode) colNode.getChild(0);
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
                stack.push((HiveParserASTNode) next.getChildren().get(i));
            }
        }
    }

    static PartitionSpec processPartitionSpec(HiveParserASTNode node) {
        PartitionSpec pSpec = new PartitionSpec();
        int exprCnt = node.getChildCount();
        for (int i = 0; i < exprCnt; i++) {
            PartitionExpression exprSpec = new PartitionExpression();
            exprSpec.setExpression((HiveParserASTNode) node.getChild(i));
            pSpec.addExpression(exprSpec);
        }
        return pSpec;
    }

    static OrderSpec processOrderSpec(HiveParserASTNode sortNode) {
        OrderSpec oSpec = new OrderSpec();
        int exprCnt = sortNode.getChildCount();
        for (int i = 0; i < exprCnt; i++) {
            OrderExpression exprSpec = new OrderExpression();
            HiveParserASTNode orderSpec = (HiveParserASTNode) sortNode.getChild(i);
            HiveParserASTNode nullOrderSpec = (HiveParserASTNode) orderSpec.getChild(0);
            exprSpec.setExpression((HiveParserASTNode) nullOrderSpec.getChild(0));
            if (orderSpec.getType() == HiveASTParser.TOK_TABSORTCOLNAMEASC) {
                exprSpec.setOrder(Order.ASC);
            } else {
                exprSpec.setOrder(Order.DESC);
            }
            if (nullOrderSpec.getType() == HiveASTParser.TOK_NULLS_FIRST) {
                exprSpec.setNullOrder(NullOrder.NULLS_FIRST);
            } else {
                exprSpec.setNullOrder(NullOrder.NULLS_LAST);
            }
            oSpec.addExpression(exprSpec);
        }
        return oSpec;
    }

    static PartitioningSpec processPTFPartitionSpec(HiveParserASTNode pSpecNode) {
        PartitioningSpec partitioning = new PartitioningSpec();
        HiveParserASTNode firstChild = (HiveParserASTNode) pSpecNode.getChild(0);
        int type = firstChild.getType();

        if (type == HiveASTParser.TOK_DISTRIBUTEBY || type == HiveASTParser.TOK_CLUSTERBY) {
            PartitionSpec pSpec = processPartitionSpec(firstChild);
            partitioning.setPartSpec(pSpec);
            HiveParserASTNode sortNode =
                    pSpecNode.getChildCount() > 1
                            ? (HiveParserASTNode) pSpecNode.getChild(1)
                            : null;
            if (sortNode != null) {
                OrderSpec oSpec = processOrderSpec(sortNode);
                partitioning.setOrderSpec(oSpec);
            }
        } else if (type == HiveASTParser.TOK_SORTBY || type == HiveASTParser.TOK_ORDERBY) {
            OrderSpec oSpec = processOrderSpec(firstChild);
            partitioning.setOrderSpec(oSpec);
        }
        return partitioning;
    }

    static HiveParserWindowingSpec.WindowFunctionSpec processWindowFunction(
            HiveParserASTNode node, HiveParserASTNode wsNode) throws SemanticException {
        HiveParserWindowingSpec.WindowFunctionSpec wfSpec =
                new HiveParserWindowingSpec.WindowFunctionSpec();

        switch (node.getType()) {
            case HiveASTParser.TOK_FUNCTIONSTAR:
                wfSpec.setStar(true);
                break;
            case HiveASTParser.TOK_FUNCTIONDI:
                wfSpec.setDistinct(true);
                break;
        }

        wfSpec.setExpression(node);

        HiveParserASTNode nameNode = (HiveParserASTNode) node.getChild(0);
        wfSpec.setName(nameNode.getText());

        for (int i = 1; i < node.getChildCount() - 1; i++) {
            HiveParserASTNode child = (HiveParserASTNode) node.getChild(i);
            wfSpec.addArg(child);
        }

        if (wsNode != null) {
            HiveParserWindowingSpec.WindowSpec ws = processWindowSpec(wsNode);
            wfSpec.setWindowSpec(ws);
        }

        return wfSpec;
    }

    static boolean containsLeadLagUDF(HiveParserASTNode expressionTree) {
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
            if (containsLeadLagUDF((HiveParserASTNode) expressionTree.getChild(i))) {
                return true;
            }
        }
        return false;
    }

    static void processQueryWindowClause(HiveParserWindowingSpec spec, HiveParserASTNode node)
            throws SemanticException {
        HiveParserASTNode nameNode = (HiveParserASTNode) node.getChild(0);
        HiveParserASTNode wsNode = (HiveParserASTNode) node.getChild(1);
        if (spec.getWindowSpecs() != null
                && spec.getWindowSpecs().containsKey(nameNode.getText())) {
            throw new SemanticException(
                    HiveParserUtils.generateErrorMessage(
                            nameNode,
                            "Duplicate definition of window "
                                    + nameNode.getText()
                                    + " is not allowed"));
        }
        HiveParserWindowingSpec.WindowSpec ws = processWindowSpec(wsNode);
        spec.addWindowSpec(nameNode.getText(), ws);
    }

    static HiveParserWindowingSpec.WindowSpec processWindowSpec(HiveParserASTNode node)
            throws SemanticException {
        boolean hasSrcId = false, hasPartSpec = false, hasWF = false;
        int srcIdIdx = -1, partIdx = -1, wfIdx = -1;

        for (int i = 0; i < node.getChildCount(); i++) {
            int type = node.getChild(i).getType();
            switch (type) {
                case HiveASTParser.Identifier:
                    hasSrcId = true;
                    srcIdIdx = i;
                    break;
                case HiveASTParser.TOK_PARTITIONINGSPEC:
                    hasPartSpec = true;
                    partIdx = i;
                    break;
                case HiveASTParser.TOK_WINDOWRANGE:
                case HiveASTParser.TOK_WINDOWVALUES:
                    hasWF = true;
                    wfIdx = i;
                    break;
            }
        }

        HiveParserWindowingSpec.WindowSpec ws = new HiveParserWindowingSpec.WindowSpec();

        if (hasSrcId) {
            HiveParserASTNode nameNode = (HiveParserASTNode) node.getChild(srcIdIdx);
            ws.setSourceId(nameNode.getText());
        }

        if (hasPartSpec) {
            HiveParserASTNode partNode = (HiveParserASTNode) node.getChild(partIdx);
            PartitioningSpec partitioning = processPTFPartitionSpec(partNode);
            ws.setPartitioning(partitioning);
        }

        if (hasWF) {
            HiveParserASTNode wfNode = (HiveParserASTNode) node.getChild(wfIdx);
            HiveParserWindowingSpec.WindowFrameSpec wfSpec = processWindowFrame(wfNode);
            ws.setWindowFrame(wfSpec);
        }
        return ws;
    }

    static HiveParserWindowingSpec.WindowFrameSpec processWindowFrame(HiveParserASTNode node)
            throws SemanticException {
        int type = node.getType();
        HiveParserWindowingSpec.BoundarySpec start = null, end = null;
        // A WindowFrame may contain just the Start Boundary or in the between style of expressing
        // a WindowFrame both boundaries are specified.
        start = processBoundary((HiveParserASTNode) node.getChild(0));
        if (node.getChildCount() > 1) {
            end = processBoundary((HiveParserASTNode) node.getChild(1));
        }
        // Note: TOK_WINDOWVALUES means RANGE type, TOK_WINDOWRANGE means ROWS type
        return new HiveParserWindowingSpec.WindowFrameSpec(
                type == HiveASTParser.TOK_WINDOWVALUES
                        ? HiveParserWindowingSpec.WindowType.RANGE
                        : HiveParserWindowingSpec.WindowType.ROWS,
                start,
                end);
    }

    static HiveParserWindowingSpec.BoundarySpec processBoundary(HiveParserASTNode node)
            throws SemanticException {
        HiveParserWindowingSpec.BoundarySpec bs = new HiveParserWindowingSpec.BoundarySpec();
        int type = node.getType();
        boolean hasAmt = true;

        switch (type) {
            case HiveASTParser.KW_PRECEDING:
                bs.setDirection(WindowingSpec.Direction.PRECEDING);
                break;
            case HiveASTParser.KW_FOLLOWING:
                bs.setDirection(WindowingSpec.Direction.FOLLOWING);
                break;
            case HiveASTParser.KW_CURRENT:
                bs.setDirection(WindowingSpec.Direction.CURRENT);
                hasAmt = false;
                break;
        }

        if (hasAmt) {
            HiveParserASTNode amtNode = (HiveParserASTNode) node.getChild(0);
            if (amtNode.getType() == HiveASTParser.KW_UNBOUNDED) {
                bs.setAmt(HiveParserWindowingSpec.BoundarySpec.UNBOUNDED_AMOUNT);
            } else {
                int amt = Integer.parseInt(amtNode.getText());
                if (amt <= 0) {
                    throw new SemanticException(
                            "Window Frame Boundary Amount must be a positive integer, provided amount is: "
                                    + amt);
                }
                bs.setAmt(amt);
            }
        }
        return bs;
    }

    public static void removeOBInSubQuery(HiveParserQBExpr qbExpr) {
        if (qbExpr == null) {
            return;
        }

        if (qbExpr.getOpcode() == HiveParserQBExpr.Opcode.NULLOP) {
            HiveParserQB subQB = qbExpr.getQB();
            HiveParserQBParseInfo parseInfo = subQB.getParseInfo();
            String alias = qbExpr.getAlias();
            Map<String, HiveParserASTNode> destToOrderBy = parseInfo.getDestToOrderBy();
            Map<String, HiveParserASTNode> destToSortBy = parseInfo.getDestToSortBy();
            final String warning =
                    "WARNING: Order/Sort by without limit in sub query or view ["
                            + alias
                            + "] is removed, as it's pointless and bad for performance.";
            if (destToOrderBy != null) {
                for (String dest : destToOrderBy.keySet()) {
                    if (parseInfo.getDestLimit(dest) == null) {
                        removeASTChild(destToOrderBy.get(dest));
                        destToOrderBy.remove(dest);
                        LOG.warn(warning);
                    }
                }
            }
            if (destToSortBy != null) {
                for (String dest : destToSortBy.keySet()) {
                    if (parseInfo.getDestLimit(dest) == null) {
                        removeASTChild(destToSortBy.get(dest));
                        destToSortBy.remove(dest);
                        LOG.warn(warning);
                    }
                }
            }
            // recursively check sub-queries
            for (String subAlias : subQB.getSubqAliases()) {
                removeOBInSubQuery(subQB.getSubqForAlias(subAlias));
            }
        } else {
            removeOBInSubQuery(qbExpr.getQBExpr1());
            removeOBInSubQuery(qbExpr.getQBExpr2());
        }
    }

    public static TableType obtainTableType(Table tabMetaData) {
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
    public static ImmutableBitSet convert(int value, int length) {
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

    public static Map<String, Integer> buildHiveColNameToInputPosMap(
            List<ExprNodeDesc> colList, HiveParserRowResolver inputRR) {
        // Build a map of Hive column Names (ExprNodeColumnDesc Name) to the positions of those
        // projections in the input
        Map<Integer, ExprNodeDesc> hashCodeToColumnDesc = new HashMap<>();
        HiveParserExprNodeDescUtils.getExprNodeColumnDesc(colList, hashCodeToColumnDesc);
        Map<String, Integer> res = new HashMap<>();
        String exprNodecolName;
        for (ExprNodeDesc exprDesc : hashCodeToColumnDesc.values()) {
            exprNodecolName = ((ExprNodeColumnDesc) exprDesc).getColumn();
            res.put(exprNodecolName, inputRR.getPosition(exprNodecolName));
        }

        return res;
    }

    public static Map<String, Integer> buildHiveToCalciteColumnMap(HiveParserRowResolver rr) {
        Map<String, Integer> map = new HashMap<>();
        for (ColumnInfo ci : rr.getRowSchema().getSignature()) {
            map.put(ci.getInternalName(), rr.getPosition(ci.getInternalName()));
        }
        return Collections.unmodifiableMap(map);
    }

    public static org.apache.calcite.util.Pair<List<CorrelationId>, ImmutableBitSet>
            getCorrelationUse(RexCall call) {
        List<CorrelationId> correlIDs = new ArrayList<>();
        ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();
        call.accept(new HiveParserUtils.CorrelationCollector(correlIDs, requiredColumns));
        if (correlIDs.isEmpty()) {
            return null;
        }
        return org.apache.calcite.util.Pair.of(correlIDs, requiredColumns.build());
    }

    public static boolean topLevelConjunctCheck(
            HiveParserASTNode searchCond, ObjectPair<Boolean, Integer> subqInfo) {
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
                    topLevelConjunctCheck((HiveParserASTNode) searchCond.getChild(i), subqInfo);
            if (!validSubQuery) {
                return false;
            }
        }
        return true;
    }

    public static void addToGBExpr(
            HiveParserRowResolver groupByOutputRowResolver,
            HiveParserRowResolver groupByInputRowResolver,
            HiveParserASTNode grpbyExpr,
            ExprNodeDesc grpbyExprNDesc,
            List<ExprNodeDesc> gbExprNDescLst,
            List<String> outputColumnNames) {
        // TODO: Should we use grpbyExprNDesc.getTypeInfo()? what if expr is UDF
        int i = gbExprNDescLst.size();
        String field = getColumnInternalName(i);
        outputColumnNames.add(field);
        gbExprNDescLst.add(grpbyExprNDesc);

        ColumnInfo outColInfo = new ColumnInfo(field, grpbyExprNDesc.getTypeInfo(), null, false);
        groupByOutputRowResolver.putExpression(grpbyExpr, outColInfo);

        addAlternateGByKeyMappings(
                grpbyExpr, outColInfo, groupByInputRowResolver, groupByOutputRowResolver);
    }

    public static int getWindowSpecIndx(HiveParserASTNode wndAST) {
        int wi = wndAST.getChildCount() - 1;
        if (wi <= 0 || (wndAST.getChild(wi).getType() != HiveASTParser.TOK_WINDOWSPEC)) {
            wi = -1;
        }
        return wi;
    }

    private static void addAlternateGByKeyMappings(
            HiveParserASTNode gByExpr,
            ColumnInfo colInfo,
            HiveParserRowResolver inputRR,
            HiveParserRowResolver outputRR) {
        if (gByExpr.getType() == HiveASTParser.DOT
                && gByExpr.getChild(0).getType() == HiveASTParser.TOK_TABLE_OR_COL) {
            String tabAlias =
                    HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                            gByExpr.getChild(0).getChild(0).getText().toLowerCase());
            String colAlias =
                    HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                            gByExpr.getChild(1).getText().toLowerCase());
            outputRR.put(tabAlias, colAlias, colInfo);
        } else if (gByExpr.getType() == HiveASTParser.TOK_TABLE_OR_COL) {
            String colAlias =
                    HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                            gByExpr.getChild(0).getText().toLowerCase());
            String tabAlias = null;
            /*
             * If the input to the GBy has a table alias for the column, then add an entry based on that tab_alias.
             * For e.g. this query: select b.x, count(*) from t1 b group by x needs (tab_alias=b, col_alias=x) in the
             * GBy RR. tab_alias=b comes from looking at the HiveParserRowResolver that is the
             * ancestor before any GBy/ReduceSinks added for the GBY operation.
             */
            try {
                ColumnInfo pColInfo = inputRR.get(tabAlias, colAlias);
                tabAlias = pColInfo == null ? null : pColInfo.getTabAlias();
            } catch (SemanticException se) {
            }
            outputRR.put(tabAlias, colAlias, colInfo);
        }
    }

    // We support having referring alias just as in hive's semantic analyzer. This check only prints
    // a warning now.
    public static void validateNoHavingReferenceToAlias(
            HiveParserQB qb,
            HiveParserASTNode havingExpr,
            HiveParserRowResolver inputRR,
            HiveParserSemanticAnalyzer semanticAnalyzer)
            throws SemanticException {
        HiveParserQBParseInfo qbPI = qb.getParseInfo();
        Map<HiveParserASTNode, String> exprToAlias = qbPI.getAllExprToColumnAlias();

        for (Map.Entry<HiveParserASTNode, String> exprAndAlias : exprToAlias.entrySet()) {
            final HiveParserASTNode expr = exprAndAlias.getKey();
            final String alias = exprAndAlias.getValue();
            // put the alias in input RR so that we can generate ExprNodeDesc with it
            if (inputRR.getExpression(expr) != null) {
                inputRR.put("", alias, inputRR.getExpression(expr));
            }
            final Set<Object> aliasReferences = new HashSet<>();

            TreeVisitorAction action =
                    new TreeVisitorAction() {
                        @Override
                        public Object pre(Object t) {
                            if (HiveASTParseDriver.ADAPTOR.getType(t)
                                    == HiveASTParser.TOK_TABLE_OR_COL) {
                                Object c = HiveASTParseDriver.ADAPTOR.getChild(t, 0);
                                if (c != null
                                        && HiveASTParseDriver.ADAPTOR.getType(c)
                                                == HiveASTParser.Identifier
                                        && HiveASTParseDriver.ADAPTOR.getText(c).equals(alias)) {
                                    aliasReferences.add(t);
                                }
                            }
                            return t;
                        }

                        @Override
                        public Object post(Object t) {
                            return t;
                        }
                    };
            new TreeVisitor(HiveASTParseDriver.ADAPTOR).visit(havingExpr, action);

            if (aliasReferences.size() > 0) {
                String havingClause =
                        semanticAnalyzer
                                .ctx
                                .getTokenRewriteStream()
                                .toString(
                                        havingExpr.getTokenStartIndex(),
                                        havingExpr.getTokenStopIndex());
                String msg =
                        String.format(
                                "Encountered Select alias '%s' in having clause '%s'"
                                        + " This is non standard behavior.",
                                alias, havingClause);
                LOG.warn(msg);
            }
        }
    }

    public static List<RexNode> getPartitionKeys(
            PartitionSpec partitionSpec,
            HiveParserRexNodeConverter converter,
            HiveParserRowResolver inputRR,
            HiveParserTypeCheckCtx typeCheckCtx,
            HiveParserSemanticAnalyzer semanticAnalyzer)
            throws SemanticException {
        List<RexNode> res = new ArrayList<>();
        if (partitionSpec != null) {
            List<PartitionExpression> expressions = partitionSpec.getExpressions();
            for (PartitionExpression expression : expressions) {
                typeCheckCtx.setAllowStatefulFunctions(true);
                ExprNodeDesc exp =
                        semanticAnalyzer.genExprNodeDesc(
                                expression.getExpression(), inputRR, typeCheckCtx);
                res.add(converter.convert(exp));
            }
        }
        return res;
    }

    public static List<RexFieldCollation> getOrderKeys(
            OrderSpec orderSpec,
            HiveParserRexNodeConverter converter,
            HiveParserRowResolver inputRR,
            HiveParserTypeCheckCtx typeCheckCtx,
            HiveParserSemanticAnalyzer semanticAnalyzer)
            throws SemanticException {
        List<RexFieldCollation> orderKeys = new ArrayList<>();
        if (orderSpec != null) {
            List<OrderExpression> oExprs = orderSpec.getExpressions();
            for (OrderExpression oExpr : oExprs) {
                typeCheckCtx.setAllowStatefulFunctions(true);
                ExprNodeDesc exp =
                        semanticAnalyzer.genExprNodeDesc(
                                oExpr.getExpression(), inputRR, typeCheckCtx);
                RexNode ordExp = converter.convert(exp);
                Set<SqlKind> flags = new HashSet<>();
                if (oExpr.getOrder() == Order.DESC) {
                    flags.add(SqlKind.DESCENDING);
                }
                if (oExpr.getNullOrder() == NullOrder.NULLS_FIRST) {
                    flags.add(SqlKind.NULLS_FIRST);
                } else if (oExpr.getNullOrder() == NullOrder.NULLS_LAST) {
                    flags.add(SqlKind.NULLS_LAST);
                } else {
                    throw new SemanticException(
                            "Unexpected null ordering option: " + oExpr.getNullOrder());
                }
                orderKeys.add(new RexFieldCollation(ordExp, flags));
            }
        }

        return orderKeys;
    }

    public static AggInfo getHiveAggInfo(
            HiveParserASTNode aggAst,
            int aggFnLstArgIndx,
            HiveParserRowResolver inputRR,
            HiveParserWindowingSpec.WindowFunctionSpec winFuncSpec,
            HiveParserSemanticAnalyzer semanticAnalyzer,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {
        AggInfo aInfo;

        // 1 Convert UDAF Params to ExprNodeDesc
        ArrayList<ExprNodeDesc> aggParameters = new ArrayList<>();
        for (int i = 1; i <= aggFnLstArgIndx; i++) {
            HiveParserASTNode paraExpr = (HiveParserASTNode) aggAst.getChild(i);
            ExprNodeDesc paraExprNode = semanticAnalyzer.genExprNodeDesc(paraExpr, inputRR);
            aggParameters.add(paraExprNode);
        }

        // 2. Is this distinct UDAF
        boolean isDistinct = aggAst.getType() == HiveASTParser.TOK_FUNCTIONDI;

        // 3. Determine type of UDAF
        TypeInfo udafRetType = null;

        // 3.1 Obtain UDAF name
        String aggName = unescapeIdentifier(aggAst.getChild(0).getText());

        boolean isAllColumns = false;

        // 3.2 Rank functions type is 'int'/'double'
        if (FunctionRegistry.isRankingFunction(aggName)) {
            if (aggName.equalsIgnoreCase("percent_rank")) {
                udafRetType = TypeInfoFactory.doubleTypeInfo;
            } else {
                udafRetType = TypeInfoFactory.intTypeInfo;
            }
            // set arguments for rank functions
            for (OrderExpression orderExpr : winFuncSpec.windowSpec.getOrder().getExpressions()) {
                aggParameters.add(
                        semanticAnalyzer.genExprNodeDesc(orderExpr.getExpression(), inputRR));
            }
        } else {
            // 3.3 Try obtaining UDAF evaluators to determine the ret type
            try {
                isAllColumns = aggAst.getType() == HiveASTParser.TOK_FUNCTIONSTAR;

                // 3.3.1 Get UDAF Evaluator
                GenericUDAFEvaluator.Mode amode =
                        HiveParserUtils.groupByDescModeToUDAFMode(
                                GroupByDesc.Mode.COMPLETE, isDistinct);

                GenericUDAFEvaluator genericUDAFEvaluator;
                if (aggName.toLowerCase().equals(FunctionRegistry.LEAD_FUNC_NAME)
                        || aggName.toLowerCase().equals(FunctionRegistry.LAG_FUNC_NAME)) {
                    ArrayList<ObjectInspector> originalParameterTypeInfos =
                            HiveParserUtils.getWritableObjectInspector(aggParameters);
                    genericUDAFEvaluator =
                            FunctionRegistry.getGenericWindowingEvaluator(
                                    aggName, originalParameterTypeInfos, isDistinct, isAllColumns);
                    HiveParserBaseSemanticAnalyzer.GenericUDAFInfo udaf =
                            HiveParserUtils.getGenericUDAFInfo(
                                    genericUDAFEvaluator, amode, aggParameters);
                    udafRetType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
                } else {
                    genericUDAFEvaluator =
                            HiveParserUtils.getGenericUDAFEvaluator(
                                    aggName,
                                    aggParameters,
                                    aggAst,
                                    isDistinct,
                                    isAllColumns,
                                    frameworkConfig.getOperatorTable());

                    // 3.3.2 Get UDAF Info using UDAF Evaluator
                    HiveParserBaseSemanticAnalyzer.GenericUDAFInfo udaf =
                            HiveParserUtils.getGenericUDAFInfo(
                                    genericUDAFEvaluator, amode, aggParameters);
                    if (HiveParserUtils.pivotResult(aggName)) {
                        udafRetType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
                    } else {
                        udafRetType = udaf.returnType;
                    }
                }
            } catch (Exception e) {
                LOG.debug(
                        "CBO: Couldn't Obtain UDAF evaluators for "
                                + aggName
                                + ", trying to translate to GenericUDF");
            }

            // 3.4 Try GenericUDF translation
            if (udafRetType == null) {
                HiveParserTypeCheckCtx tcCtx =
                        new HiveParserTypeCheckCtx(inputRR, frameworkConfig, cluster);
                // We allow stateful functions in the SELECT list (but nowhere else)
                tcCtx.setAllowStatefulFunctions(true);
                tcCtx.setAllowDistinctFunctions(false);
                ExprNodeDesc exp =
                        semanticAnalyzer.genExprNodeDesc(
                                (HiveParserASTNode) aggAst.getChild(0), inputRR, tcCtx);
                udafRetType = exp.getTypeInfo();
            }
        }

        // 4. Construct AggInfo
        aInfo = new AggInfo(aggParameters, udafRetType, aggName, isDistinct, isAllColumns, null);

        return aInfo;
    }

    public static RelNode genValues(
            String tabAlias,
            Table tmpTable,
            HiveParserRowResolver rowResolver,
            RelOptCluster cluster,
            List<List<String>> values) {
        List<TypeInfo> tmpTableTypes =
                tmpTable.getCols().stream()
                        .map(f -> TypeInfoUtils.getTypeInfoFromTypeString(f.getType()))
                        .collect(Collectors.toList());

        RexBuilder rexBuilder = cluster.getRexBuilder();
        // calcite types for each field
        List<RelDataType> calciteTargetTypes =
                tmpTableTypes.stream()
                        .map(
                                ti ->
                                        HiveParserTypeConverter.convert(
                                                (PrimitiveTypeInfo) ti,
                                                rexBuilder.getTypeFactory()))
                        .collect(Collectors.toList());
        // calcite field names
        List<String> calciteFieldNames =
                IntStream.range(0, calciteTargetTypes.size())
                        .mapToObj(SqlUtil::deriveAliasFromOrdinal)
                        .collect(Collectors.toList());

        // calcite type for each row
        List<RelDataType> calciteRowTypes = new ArrayList<>();

        List<List<RexLiteral>> rows = new ArrayList<>();
        for (List<String> value : values) {
            Preconditions.checkArgument(
                    value.size() == tmpTableTypes.size(),
                    String.format(
                            "Values table col length (%d) and data length (%d) mismatch",
                            tmpTableTypes.size(), value.size()));
            List<RexLiteral> row = new ArrayList<>();
            for (int i = 0; i < tmpTableTypes.size(); i++) {
                PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) tmpTableTypes.get(i);
                RelDataType calciteType = calciteTargetTypes.get(i);
                String col = value.get(i);
                if (col == null) {
                    row.add(rexBuilder.makeNullLiteral(calciteType));
                } else {
                    switch (primitiveTypeInfo.getPrimitiveCategory()) {
                        case BYTE:
                        case SHORT:
                        case INT:
                        case LONG:
                            row.add(rexBuilder.makeExactLiteral(new BigDecimal(col), calciteType));
                            break;
                        case DECIMAL:
                            BigDecimal bigDec = new BigDecimal(col);
                            row.add(
                                    SqlTypeUtil.isValidDecimalValue(bigDec, calciteType)
                                            ? rexBuilder.makeExactLiteral(bigDec, calciteType)
                                            : rexBuilder.makeNullLiteral(calciteType));
                            break;
                        case FLOAT:
                        case DOUBLE:
                            row.add(rexBuilder.makeApproxLiteral(new BigDecimal(col), calciteType));
                            break;
                        case BOOLEAN:
                            row.add(rexBuilder.makeLiteral(Boolean.parseBoolean(col)));
                            break;
                        default:
                            row.add(
                                    rexBuilder.makeCharLiteral(
                                            HiveParserUtils.asUnicodeString(col)));
                    }
                }
            }

            calciteRowTypes.add(
                    rexBuilder
                            .getTypeFactory()
                            .createStructType(
                                    row.stream()
                                            .map(RexLiteral::getType)
                                            .collect(Collectors.toList()),
                                    calciteFieldNames));
            rows.add(row);
        }

        // compute the final row type
        RelDataType calciteRowType = rexBuilder.getTypeFactory().leastRestrictive(calciteRowTypes);
        for (int i = 0; i < calciteFieldNames.size(); i++) {
            ColumnInfo colInfo =
                    new ColumnInfo(
                            calciteFieldNames.get(i),
                            HiveParserTypeConverter.convert(
                                    calciteRowType.getFieldList().get(i).getType()),
                            tabAlias,
                            false);
            rowResolver.put(tabAlias, calciteFieldNames.get(i), colInfo);
        }
        return HiveParserUtils.genValuesRelNode(
                cluster,
                rexBuilder.getTypeFactory().createStructType(calciteRowType.getFieldList()),
                rows);
    }

    private static void validatePartColumnType(
            Table tbl,
            Map<String, String> partSpec,
            HiveParserASTNode astNode,
            HiveConf conf,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {
        if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_TYPE_CHECK_ON_INSERT)) {
            return;
        }

        Map<HiveParserASTNode, ExprNodeDesc> astExprNodeMap = new HashMap<>();
        if (!getPartExprNodeDesc(astNode, conf, astExprNodeMap, frameworkConfig, cluster)) {
            LOG.warn(
                    "Dynamic partitioning is used; only validating "
                            + astExprNodeMap.size()
                            + " columns");
        }

        if (astExprNodeMap.isEmpty()) {
            return; // All columns are dynamic, nothing to do.
        }

        List<FieldSchema> parts = tbl.getPartitionKeys();
        Map<String, String> partCols = new HashMap<>(parts.size());
        for (FieldSchema col : parts) {
            partCols.put(col.getName(), col.getType().toLowerCase());
        }
        for (Map.Entry<HiveParserASTNode, ExprNodeDesc> astExprNodePair :
                astExprNodeMap.entrySet()) {
            String astKeyName = astExprNodePair.getKey().toString().toLowerCase();
            if (astExprNodePair.getKey().getType() == HiveASTParser.Identifier) {
                astKeyName = stripIdentifierQuotes(astKeyName);
            }
            String colType = partCols.get(astKeyName);
            ObjectInspector inputOI =
                    TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
                            astExprNodePair.getValue().getTypeInfo());

            TypeInfo expectedType = TypeInfoUtils.getTypeInfoFromTypeString(colType);
            ObjectInspector outputOI =
                    TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(expectedType);
            //  Since partVal is a constant, it is safe to cast ExprNodeDesc to
            // ExprNodeConstantDesc.
            //  Its value should be in normalized format (e.g. no leading zero in integer, date is
            // in
            //  format of YYYY-MM-DD etc)
            Object value = ((ExprNodeConstantDesc) astExprNodePair.getValue()).getValue();
            Object convertedValue = value;
            if (!inputOI.getTypeName().equals(outputOI.getTypeName())) {
                convertedValue =
                        ObjectInspectorConverters.getConverter(inputOI, outputOI).convert(value);
                if (convertedValue == null) {
                    throw new SemanticException(
                            ErrorMsg.PARTITION_SPEC_TYPE_MISMATCH,
                            astKeyName,
                            inputOI.getTypeName(),
                            outputOI.getTypeName());
                }

                if (!convertedValue.toString().equals(value.toString())) {
                    //  value might have been changed because of the normalization in conversion
                    LOG.warn(
                            "Partition "
                                    + astKeyName
                                    + " expects type "
                                    + outputOI.getTypeName()
                                    + " but input value is in type "
                                    + inputOI.getTypeName()
                                    + ". Convert "
                                    + value.toString()
                                    + " to "
                                    + convertedValue.toString());
                }
            }

            if (!convertedValue.toString().equals(partSpec.get(astKeyName))) {
                LOG.warn(
                        "Partition Spec "
                                + astKeyName
                                + "="
                                + partSpec.get(astKeyName)
                                + " has been changed to "
                                + astKeyName
                                + "="
                                + convertedValue.toString());
            }
            partSpec.put(astKeyName, convertedValue.toString());
        }
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

    /** Counterpart of hive's BaseSemanticAnalyzer.TableSpec. */
    public static class TableSpec {
        public String tableName;
        public Table tableHandle;
        public Map<String, String> partSpec; // has to use LinkedHashMap to enforce order
        public Partition partHandle;
        public int numDynParts; // number of dynamic partition columns
        public List<Partition>
                partitions; // involved partitions in TableScanOperator/FileSinkOperator

        /** SpecType. */
        public enum SpecType {
            TABLE_ONLY,
            STATIC_PARTITION,
            DYNAMIC_PARTITION
        }

        public TableSpec.SpecType specType;

        public TableSpec(
                Hive db,
                HiveConf conf,
                HiveParserASTNode ast,
                FrameworkConfig frameworkConfig,
                RelOptCluster cluster)
                throws SemanticException {
            this(db, conf, ast, true, false, frameworkConfig, cluster);
        }

        public TableSpec(
                Hive db,
                HiveConf conf,
                HiveParserASTNode ast,
                boolean allowDynamicPartitionsSpec,
                boolean allowPartialPartitionsSpec,
                FrameworkConfig frameworkConfig,
                RelOptCluster cluster)
                throws SemanticException {
            assert (ast.getToken().getType() == HiveASTParser.TOK_TAB
                    || ast.getToken().getType() == HiveASTParser.TOK_TABLE_PARTITION
                    || ast.getToken().getType() == HiveASTParser.TOK_TABTYPE
                    || ast.getToken().getType() == HiveASTParser.TOK_CREATETABLE
                    || ast.getToken().getType() == HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW);
            int childIndex = 0;
            numDynParts = 0;

            try {
                // get table metadata
                tableName = getUnescapedName((HiveParserASTNode) ast.getChild(0));
                boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
                if (testMode) {
                    tableName = conf.getVar(HiveConf.ConfVars.HIVETESTMODEPREFIX) + tableName;
                }
                if (ast.getToken().getType() != HiveASTParser.TOK_CREATETABLE
                        && ast.getToken().getType() != HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW) {
                    tableHandle = db.getTable(tableName);
                }
            } catch (InvalidTableException ite) {
                throw new SemanticException(
                        HiveParserErrorMsg.getMsg(ErrorMsg.INVALID_TABLE, ast.getChild(0)), ite);
            } catch (HiveException e) {
                throw new SemanticException("Error while retrieving table metadata", e);
            }

            // get partition metadata if partition specified
            if (ast.getChildCount() == 2
                    && ast.getToken().getType() != HiveASTParser.TOK_CREATETABLE
                    && ast.getToken().getType() != HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW) {
                childIndex = 1;
                HiveParserASTNode partspec = (HiveParserASTNode) ast.getChild(1);
                partitions = new ArrayList<Partition>();
                // partSpec is a mapping from partition column name to its value.
                Map<String, String> tmpPartSpec = new HashMap<>(partspec.getChildCount());
                for (int i = 0; i < partspec.getChildCount(); ++i) {
                    HiveParserASTNode partspecVal = (HiveParserASTNode) partspec.getChild(i);
                    String val = null;
                    String colName =
                            unescapeIdentifier(partspecVal.getChild(0).getText().toLowerCase());
                    if (partspecVal.getChildCount() < 2) { // DP in the form of T partition (ds, hr)
                        if (allowDynamicPartitionsSpec) {
                            ++numDynParts;
                        } else {
                            throw new SemanticException(
                                    ErrorMsg.INVALID_PARTITION.getMsg(
                                            " - Dynamic partitions not allowed"));
                        }
                    } else { // in the form of T partition (ds="2010-03-03")
                        val = stripQuotes(partspecVal.getChild(1).getText());
                    }
                    tmpPartSpec.put(colName, val);
                }

                // check if the columns, as well as value types in the partition() clause are valid
                validatePartSpec(
                        tableHandle, tmpPartSpec, ast, conf, false, frameworkConfig, cluster);

                List<FieldSchema> parts = tableHandle.getPartitionKeys();
                partSpec = new LinkedHashMap<String, String>(partspec.getChildCount());
                for (FieldSchema fs : parts) {
                    String partKey = fs.getName();
                    partSpec.put(partKey, tmpPartSpec.get(partKey));
                }

                // check if the partition spec is valid
                if (numDynParts > 0) {
                    int numStaPart = parts.size() - numDynParts;
                    if (numStaPart == 0
                            && conf.getVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE)
                                    .equalsIgnoreCase("strict")) {
                        throw new SemanticException(
                                ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg());
                    }

                    // check the partitions in partSpec be the same as defined in table schema
                    if (partSpec.keySet().size() != parts.size()) {
                        errorPartSpec(partSpec, parts);
                    }
                    Iterator<String> itrPsKeys = partSpec.keySet().iterator();
                    for (FieldSchema fs : parts) {
                        if (!itrPsKeys.next().toLowerCase().equals(fs.getName().toLowerCase())) {
                            errorPartSpec(partSpec, parts);
                        }
                    }

                    // check if static partition appear after dynamic partitions
                    for (FieldSchema fs : parts) {
                        if (partSpec.get(fs.getName().toLowerCase()) == null) {
                            if (numStaPart > 0) { // found a DP, but there exists ST as subpartition
                                throw new SemanticException(
                                        HiveParserErrorMsg.getMsg(
                                                ErrorMsg.PARTITION_DYN_STA_ORDER,
                                                ast.getChild(childIndex)));
                            }
                            break;
                        } else {
                            --numStaPart;
                        }
                    }
                    partHandle = null;
                    specType = TableSpec.SpecType.DYNAMIC_PARTITION;
                } else {
                    try {
                        if (allowPartialPartitionsSpec) {
                            partitions = db.getPartitions(tableHandle, partSpec);
                        } else {
                            // this doesn't create partition.
                            partHandle = db.getPartition(tableHandle, partSpec, false);
                            if (partHandle == null) {
                                // if partSpec doesn't exists in DB, return a delegate one
                                // and the actual partition is created in MoveTask
                                partHandle = new Partition(tableHandle, partSpec, null);
                            } else {
                                partitions.add(partHandle);
                            }
                        }
                    } catch (HiveException e) {
                        throw new SemanticException(
                                HiveParserErrorMsg.getMsg(
                                        ErrorMsg.INVALID_PARTITION, ast.getChild(childIndex)),
                                e);
                    }
                    specType = TableSpec.SpecType.STATIC_PARTITION;
                }
            } else {
                specType = TableSpec.SpecType.TABLE_ONLY;
            }
        }

        public Map<String, String> getPartSpec() {
            return this.partSpec;
        }

        public void setPartSpec(Map<String, String> partSpec) {
            this.partSpec = partSpec;
        }

        @Override
        public String toString() {
            if (partHandle != null) {
                return partHandle.toString();
            } else {
                return tableHandle.toString();
            }
        }
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

        public PKInfo(String colName) {
            this.colName = colName;
        }

        public PKInfo(String colName, String constraintName, boolean rely) {
            this.colName = colName;
            this.constraintName = constraintName;
            this.rely = rely;
        }
    }

    /** Counterpart of hive's SemanticAnalyzer.CTEClause. */
    static class CTEClause {
        CTEClause(String alias, HiveParserASTNode cteNode) {
            this.alias = alias;
            this.cteNode = cteNode;
        }

        String alias;
        HiveParserASTNode cteNode;
        boolean materialize;
        int reference;
        HiveParserQBExpr qbExpr;
        List<CTEClause> parents = new ArrayList<>();

        @Override
        public String toString() {
            return alias == null ? "<root>" : alias;
        }
    }

    /** Counterpart of hive's SemanticAnalyzer.Phase1Ctx. */
    public static class Phase1Ctx {
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
    public static class AggInfo {
        private final List<ExprNodeDesc> aggParams;
        private final TypeInfo returnType;
        private final String udfName;
        private final boolean distinct;
        private final boolean isAllColumns;
        private final String alias;

        public AggInfo(
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

        public void analyzeRowFormat(HiveParserASTNode child) throws SemanticException {
            child = (HiveParserASTNode) child.getChild(0);
            int numChildRowFormat = child.getChildCount();
            for (int numC = 0; numC < numChildRowFormat; numC++) {
                HiveParserASTNode rowChild = (HiveParserASTNode) child.getChild(numC);
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

    /** Counterpart of hive's SQLPrimaryKey. */
    public static class PrimaryKey implements Serializable {

        private static final long serialVersionUID = 3036210046732750293L;

        private final String dbName;
        private final String tblName;
        private final String pk;
        private final String constraintName;
        private final boolean enable;
        private final boolean validate;
        private final boolean rely;

        public PrimaryKey(
                String dbName,
                String tblName,
                String pk,
                String constraintName,
                boolean enable,
                boolean validate,
                boolean rely) {
            this.dbName = dbName;
            this.tblName = tblName;
            this.pk = pk;
            this.constraintName = constraintName;
            this.enable = enable;
            this.validate = validate;
            this.rely = rely;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTblName() {
            return tblName;
        }

        public String getPk() {
            return pk;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public boolean isEnable() {
            return enable;
        }

        public boolean isValidate() {
            return validate;
        }

        public boolean isRely() {
            return rely;
        }
    }

    /** Counterpart of hive's SQLNotNullConstraint. */
    public static class NotNullConstraint implements Serializable {

        private static final long serialVersionUID = 7642343368203203950L;

        private final String dbName;
        private final String tblName;
        private final String colName;
        private final String constraintName;
        private final boolean enable;
        private final boolean validate;
        private final boolean rely;

        public NotNullConstraint(
                String dbName,
                String tblName,
                String colName,
                String constraintName,
                boolean enable,
                boolean validate,
                boolean rely) {
            this.dbName = dbName;
            this.tblName = tblName;
            this.colName = colName;
            this.constraintName = constraintName;
            this.enable = enable;
            this.validate = validate;
            this.rely = rely;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTblName() {
            return tblName;
        }

        public String getColName() {
            return colName;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public boolean isEnable() {
            return enable;
        }

        public boolean isValidate() {
            return validate;
        }

        public boolean isRely() {
            return rely;
        }
    }
}
