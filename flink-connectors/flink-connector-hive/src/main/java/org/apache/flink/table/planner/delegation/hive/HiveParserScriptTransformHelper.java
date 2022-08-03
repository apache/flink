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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQB;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserRowResolver;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserTypeConverter;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.plan.nodes.hive.LogicalScriptTransform;
import org.apache.flink.table.runtime.script.ScriptTransformIOInfo;

import org.antlr.runtime.tree.Tree;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.TextRecordReader;
import org.apache.hadoop.hive.ql.exec.TextRecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.buildHiveToCalciteColumnMap;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getColumnInternalName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getTypeStringFromAST;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unescapeSQLString;

/** A helper class to handle script transform in hive dialect. */
public class HiveParserScriptTransformHelper {

    private final LinkedHashMap<RelNode, HiveParserRowResolver> relToRowResolver;
    private final HiveConf hiveConf;
    private final LinkedHashMap<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap;
    private final RelOptCluster cluster;

    public HiveParserScriptTransformHelper(
            RelOptCluster cluster,
            LinkedHashMap<RelNode, HiveParserRowResolver> relToRowResolver,
            LinkedHashMap<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap,
            HiveConf hiveConf) {
        this.cluster = cluster;
        this.relToRowResolver = relToRowResolver;
        this.relToHiveColNameCalcitePosMap = relToHiveColNameCalcitePosMap;
        this.hiveConf = hiveConf;
    }

    public RelNode genScriptPlan(
            HiveParserASTNode trfm, HiveParserQB qb, List<RexNode> operands, RelNode input)
            throws SemanticException {
        // if there's any RexNode is not RexInputRef,
        // create a project node before create the script transform node
        boolean isAllRexRef = operands.stream().allMatch(node -> node instanceof RexInputRef);
        int[] transformFieldIndices;
        if (!isAllRexRef) {
            input =
                    LogicalProject.create(
                            input, Collections.emptyList(), operands, (List<String>) null);
            transformFieldIndices = IntStream.range(0, operands.size()).toArray();
            HiveParserRowResolver rowResolver = new HiveParserRowResolver();
            // record the column info for the project node
            for (int i = 0; i < operands.size(); i++) {
                ColumnInfo oColInfo =
                        new ColumnInfo(
                                getColumnInternalName(i),
                                HiveParserTypeConverter.convert(operands.get(i).getType()),
                                null,
                                false);
                rowResolver.put(null, getColumnInternalName(i), oColInfo);
            }
            relToRowResolver.put(input, rowResolver);
        } else {
            transformFieldIndices =
                    operands.stream()
                            .flatMapToInt(node -> IntStream.of(((RexInputRef) node).getIndex()))
                            .toArray();
        }

        ArrayList<ColumnInfo> inputSchema = relToRowResolver.get(input).getColumnInfos();

        // If there is no "AS" clause, the output schema will be "key,value"
        ArrayList<ColumnInfo> outputCols = new ArrayList<>();
        int inputSerDeNum = 1, inputRecordWriterNum = 2;
        int outputSerDeNum = 4, outputRecordReaderNum = 5;
        int outputColsNum = 6;
        boolean outputColNames = false, outputColSchemas = false;
        int execPos = 3;
        boolean defaultOutputCols = false;

        // Go over all the children
        if (trfm.getChildCount() > outputColsNum) {
            HiveParserASTNode outCols = (HiveParserASTNode) trfm.getChild(outputColsNum);
            if (outCols.getType() == HiveASTParser.TOK_ALIASLIST) {
                outputColNames = true;
            } else if (outCols.getType() == HiveASTParser.TOK_TABCOLLIST) {
                outputColSchemas = true;
            }
        }

        // If column type is not specified, use a string
        if (!outputColNames && !outputColSchemas) {
            // output schema will be "key, value"
            String[] outputAlias = new String[] {"key", "value"};
            for (int i = 0; i < outputAlias.length; i++) {
                String intName = getColumnInternalName(i);
                ColumnInfo colInfo =
                        new ColumnInfo(intName, TypeInfoFactory.stringTypeInfo, null, false);
                colInfo.setAlias(outputAlias[i]);
                outputCols.add(colInfo);
            }
            defaultOutputCols = true;
        } else {
            // column name or type is specified
            HiveParserASTNode collist = (HiveParserASTNode) trfm.getChild(outputColsNum);
            int ccount = collist.getChildCount();
            Set<String> colAliasNamesDuplicateCheck = new HashSet<>();
            for (int i = 0; i < ccount; i++) {
                ColumnInfo colInfo =
                        getColumnInfoInScriptTransform(
                                (HiveParserASTNode) collist.getChild(i),
                                outputColSchemas,
                                i,
                                colAliasNamesDuplicateCheck);
                outputCols.add(colInfo);
            }
        }

        // input schema info
        StringBuilder inpColumns = new StringBuilder();
        StringBuilder inpColumnTypes = new StringBuilder();
        for (int i = 0; i < transformFieldIndices.length; i++) {
            if (i != 0) {
                inpColumns.append(",");
                inpColumnTypes.append(",");
            }
            inpColumns.append(inputSchema.get(transformFieldIndices[i]).getInternalName());
            inpColumnTypes.append(
                    inputSchema.get(transformFieldIndices[i]).getType().getTypeName());
        }

        // output schema info
        StringBuilder outColumns = new StringBuilder();
        StringBuilder outColumnTypes = new StringBuilder();
        List<RelDataType> outDataTypes = new ArrayList<>();
        List<String> outColNames = new ArrayList<>();
        HiveParserRowResolver scriptRR = new HiveParserRowResolver();
        RelDataTypeFactory dtFactory = cluster.getRexBuilder().getTypeFactory();
        for (int i = 0; i < outputCols.size(); i++) {
            if (i != 0) {
                outColumns.append(",");
                outColumnTypes.append(",");
            }

            outColumns.append(outputCols.get(i).getInternalName());
            outColumnTypes.append(outputCols.get(i).getType().getTypeName());

            scriptRR.put(
                    qb.getParseInfo().getAlias(), outputCols.get(i).getAlias(), outputCols.get(i));

            outDataTypes.add(HiveParserUtils.toRelDataType(outputCols.get(i).getType(), dtFactory));
            outColNames.add(outputCols.get(i).getInternalName());
        }

        String serdeName = LazySimpleSerDe.class.getName();
        int fieldSeparator = Utilities.tabCode;
        if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVESCRIPTESCAPE)) {
            fieldSeparator = Utilities.ctrlaCode;
        }

        // Input and Output Serdes
        HiveParserBaseSemanticAnalyzer.SerDeClassProps inSerDeClassProps;
        if (trfm.getChild(inputSerDeNum).getChildCount() > 0) {
            // use user specified serialize class and properties
            HiveParserASTNode inputSerDeNode = (HiveParserASTNode) trfm.getChild(inputSerDeNum);
            inSerDeClassProps =
                    HiveParserBaseSemanticAnalyzer.SerDeClassProps.analyzeSerDeInfo(
                            (HiveParserASTNode) inputSerDeNode.getChild(0),
                            inpColumns.toString(),
                            inpColumnTypes.toString(),
                            false);
        } else {
            // use default serialize class and properties
            Map<String, String> inSerdeProps =
                    HiveParserBaseSemanticAnalyzer.SerDeClassProps.getDefaultSerDeProps(
                            serdeName,
                            String.valueOf(fieldSeparator),
                            inpColumns.toString(),
                            inpColumnTypes.toString(),
                            false,
                            true);
            inSerDeClassProps =
                    new HiveParserBaseSemanticAnalyzer.SerDeClassProps(serdeName, inSerdeProps);
        }
        HiveParserBaseSemanticAnalyzer.SerDeClassProps outSerDeClassProps;
        if (trfm.getChild(outputSerDeNum).getChildCount() > 0) {
            // use user specified deserialize class and properties
            HiveParserASTNode outSerDeNode = (HiveParserASTNode) trfm.getChild(outputSerDeNum);
            outSerDeClassProps =
                    HiveParserBaseSemanticAnalyzer.SerDeClassProps.analyzeSerDeInfo(
                            (HiveParserASTNode) outSerDeNode.getChild(0),
                            outColumns.toString(),
                            outColumnTypes.toString(),
                            false);
        } else {
            // use default deserialize class and properties
            Map<String, String> outSerdeProps =
                    HiveParserBaseSemanticAnalyzer.SerDeClassProps.getDefaultSerDeProps(
                            serdeName,
                            String.valueOf(fieldSeparator),
                            outColumns.toString(),
                            outColumnTypes.toString(),
                            defaultOutputCols,
                            true);
            outSerDeClassProps =
                    new HiveParserBaseSemanticAnalyzer.SerDeClassProps(serdeName, outSerdeProps);
        }

        // script input record writer
        Tree recordWriterASTNode = trfm.getChild(inputRecordWriterNum);
        String inRecordWriter =
                recordWriterASTNode.getChildCount() == 0
                        ? TextRecordWriter.class.getName()
                        : unescapeSQLString(recordWriterASTNode.getChild(0).getText());

        // script output record readers
        Tree recordReaderASTNode = trfm.getChild(outputRecordReaderNum);
        String outRecordReader =
                recordReaderASTNode.getChildCount() == 0
                        ? TextRecordReader.class.getName()
                        : unescapeSQLString(recordReaderASTNode.getChild(0).getText());

        RelDataType rowDataType = dtFactory.createStructType(outDataTypes, outColNames);

        String script = unescapeSQLString(trfm.getChild(execPos).getText());

        ScriptTransformIOInfo inputOutSchema =
                new ScriptTransformIOInfo(
                        inSerDeClassProps.getSerdeClassName(),
                        inSerDeClassProps.getProperties(),
                        outSerDeClassProps.getSerdeClassName(),
                        outSerDeClassProps.getProperties(),
                        inRecordWriter,
                        outRecordReader,
                        new JobConfWrapper(new JobConf(hiveConf)));

        LogicalScriptTransform scriptTransform =
                LogicalScriptTransform.create(
                        input, transformFieldIndices, script, inputOutSchema, rowDataType);

        relToHiveColNameCalcitePosMap.put(scriptTransform, buildHiveToCalciteColumnMap(scriptRR));
        relToRowResolver.put(scriptTransform, scriptRR);

        // todo
        // Add URI entity for transform script. script assumed t be local unless downloadable
        return scriptTransform;
    }

    private ColumnInfo getColumnInfoInScriptTransform(
            HiveParserASTNode node,
            boolean outputColSchemas,
            int pos,
            Set<String> colAliasNamesDuplicateCheck)
            throws SemanticException {
        if (outputColSchemas) {
            // specific col, col_type
            String colAlias = unescapeIdentifier(node.getChild(0).getText()).toLowerCase();
            failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
            String intName = getColumnInternalName(pos);
            ColumnInfo colInfo =
                    new ColumnInfo(
                            intName,
                            TypeInfoUtils.getTypeInfoFromTypeString(
                                    getTypeStringFromAST((HiveParserASTNode) node.getChild(1))),
                            null,
                            false);
            colInfo.setAlias(colAlias);
            return colInfo;
        } else {
            // only specific col, all type will be string
            String colAlias = unescapeIdentifier(node.getText()).toLowerCase();
            failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
            String intName = getColumnInternalName(pos);
            ColumnInfo colInfo =
                    new ColumnInfo(intName, TypeInfoFactory.stringTypeInfo, null, false);
            colInfo.setAlias(colAlias);
            return colInfo;
        }
    }

    private void failIfColAliasExists(Set<String> nameSet, String name) throws SemanticException {
        if (nameSet.contains(name)) {
            throw new SemanticException(ErrorMsg.COLUMN_ALIAS_ALREADY_EXISTS.getMsg(name));
        }
        nameSet.add(name);
    }
}
