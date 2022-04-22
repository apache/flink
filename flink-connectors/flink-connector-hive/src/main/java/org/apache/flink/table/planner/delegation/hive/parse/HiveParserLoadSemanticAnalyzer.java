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

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.TableSpec;
import org.apache.flink.table.planner.delegation.hive.operation.HiveLoadDataOperation;
import org.apache.flink.util.StringUtils;

import org.antlr.runtime.tree.Tree;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.hive.HiveCatalog.TEMP_TABLE_FOR_LOAD_DATA_NAME_SUFFIX;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getDotName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getQualifiedTableName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.stripQuotes;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;

/** Ported hive's org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer. */
public class HiveParserLoadSemanticAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(HiveParserLoadSemanticAnalyzer.class);

    private final HiveCatalog hiveCatalog;
    private final HiveConf conf;
    private final Hive db;
    private final FrameworkConfig frameworkConfig;
    private final RelOptCluster cluster;

    private final String inputFormatClassName = null;
    private final String serDeClassName = null;

    private boolean needRewrite;

    public HiveParserLoadSemanticAnalyzer(
            HiveCatalog hiveCatalog,
            HiveConf conf,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {
        this.hiveCatalog = hiveCatalog;
        this.conf = conf;
        try {
            this.db = Hive.get(conf);
        } catch (HiveException e) {
            throw new SemanticException(e);
        }
        this.frameworkConfig = frameworkConfig;
        this.cluster = cluster;
    }

    public HiveLoadDataOperation convertToOperation(HiveParserASTNode ast)
            throws SemanticException {
        boolean isLocal = false;
        boolean isOverWrite = false;
        Tree fromTree = ast.getChild(0);
        HiveParserASTNode tableTree = (HiveParserASTNode) ast.getChild(1);

        if (ast.getChildCount() == 4) {
            isLocal = true;
            isOverWrite = true;
        }

        if (ast.getChildCount() == 3) {
            if (ast.getChild(2).getText().equalsIgnoreCase("local")) {
                isLocal = true;
            } else {
                isOverWrite = true;
            }
        }

        // initialize load path
        URI fromURI;
        try {
            String fromPath = stripQuotes(fromTree.getText());
            fromURI = initializeFromURI(fromPath, isLocal);
        } catch (IOException | URISyntaxException e) {
            throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(fromTree, e.getMessage()), e);
        }

        // initialize destination table/partition
        TableSpec ts = new TableSpec(db, conf, tableTree, frameworkConfig, cluster);

        if (ts.tableHandle.isView() || ts.tableHandle.isMaterializedView()) {
            throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
        }
        if (ts.tableHandle.isNonNative()) {
            throw new SemanticException(ErrorMsg.LOAD_INTO_NON_NATIVE.getMsg());
        }

        if (ts.tableHandle.isStoredAsSubDirectories()) {
            throw new SemanticException(ErrorMsg.LOAD_INTO_STORED_AS_DIR.getMsg());
        }

        List<FieldSchema> parts = ts.tableHandle.getPartitionKeys();
        if ((parts != null && parts.size() > 0)
                && (ts.partSpec == null || ts.partSpec.size() == 0)) {
            // if no partition is specified in "LOAD DATA" statement,
            // need to rewrite it to "INSERT AS SELECT"
            return rewriteLoadDataSQL(hiveCatalog, ts.tableHandle, fromURI, tableTree, isOverWrite);
        }

        List<String> bucketCols = ts.tableHandle.getBucketCols();
        if (bucketCols != null && !bucketCols.isEmpty()) {
            String error = HiveConf.StrictChecks.checkBucketing(conf);
            if (error != null) {
                throw new SemanticException(
                        "Please load into an intermediate table"
                                + " and use 'insert... select' to allow Hive to enforce bucketing. "
                                + error);
            }
        }

        // make sure the arguments make sense, may need to write "LOAD DATA" to "INSERT AS SELECT"
        // when there's any directory in the fromURL
        List<FileStatus> files = applyConstraintsAndGetFiles(fromURI, fromTree, isLocal);

        if (needRewrite) {
            return rewriteLoadDataSQL(hiveCatalog, ts.tableHandle, fromURI, tableTree, isOverWrite);
        }

        // for managed tables, make sure the file formats match
        if (TableType.MANAGED_TABLE.equals(ts.tableHandle.getTableType())
                && conf.getBoolVar(HiveConf.ConfVars.HIVECHECKFILEFORMAT)) {
            ensureFileFormatsMatch(ts, files, fromURI);
        }

        return new HiveLoadDataOperation(
                new Path(fromURI),
                new ObjectPath(ts.tableHandle.getDbName(), ts.tableHandle.getTableName()),
                isOverWrite,
                isLocal,
                ts.partSpec == null ? new LinkedHashMap<>() : ts.partSpec);
    }

    private List<FileStatus> applyConstraintsAndGetFiles(URI fromURI, Tree ast, boolean isLocal)
            throws SemanticException {

        FileStatus[] srcs;

        // local mode implies that scheme should be "file"
        // we can change this going forward
        if (isLocal && !fromURI.getScheme().equals("file")) {
            throw new SemanticException(
                    ErrorMsg.ILLEGAL_PATH.getMsg(
                            ast,
                            "Source file system should be \"file\" if \"local\" is specified"));
        }

        try {
            srcs = matchFilesOrDir(FileSystem.get(fromURI, conf), new Path(fromURI));
            if (srcs == null || srcs.length == 0) {
                throw new SemanticException(
                        HiveParserErrorMsg.getMsg(
                                ErrorMsg.INVALID_PATH, ast, "No files matching path " + fromURI));
            }

            for (FileStatus oneSrc : srcs) {
                if (oneSrc.isDir()) {
                    // there's a directory in the input path, so need to rewrite the "LOAD DATA"
                    // statement
                    needRewrite = true;
                    return Collections.emptyList();
                }
            }
        } catch (IOException e) {
            throw new SemanticException(HiveParserErrorMsg.getMsg(ErrorMsg.INVALID_PATH, ast), e);
        }

        return Arrays.asList(srcs);
    }

    public static FileStatus[] matchFilesOrDir(FileSystem fs, Path path) throws IOException {
        FileStatus[] srcs =
                fs.globStatus(
                        path,
                        p -> {
                            String name = p.getName();
                            return name.equals(EximUtil.METADATA_NAME)
                                    || !name.startsWith("_") && !name.startsWith(".");
                        });
        if ((srcs != null) && srcs.length == 1) {
            if (srcs[0].isDir()) {
                srcs =
                        fs.listStatus(
                                srcs[0].getPath(),
                                p -> {
                                    String name = p.getName();
                                    return !name.startsWith("_") && !name.startsWith(".");
                                });
            }
        }
        return (srcs);
    }

    private URI initializeFromURI(String fromPath, boolean isLocal)
            throws IOException, URISyntaxException, SemanticException {
        URI fromURI = new Path(fromPath).toUri();

        String fromScheme = fromURI.getScheme();
        String fromAuthority = fromURI.getAuthority();
        String path = fromURI.getPath();

        // generate absolute path relative to current directory or hdfs home
        // directory
        if (!path.startsWith("/")) {
            if (isLocal) {
                try {
                    path =
                            new String(
                                    URLCodec.decodeUrl(
                                            new Path(System.getProperty("user.dir"), fromPath)
                                                    .toUri()
                                                    .toString()
                                                    .getBytes(StandardCharsets.US_ASCII)),
                                    StandardCharsets.US_ASCII);
                } catch (DecoderException de) {
                    throw new SemanticException("URL Decode failed", de);
                }
            } else {
                path =
                        new Path(new Path("/user/" + System.getProperty("user.name")), path)
                                .toString();
            }
        }

        // set correct scheme and authority
        if (StringUtils.isNullOrWhitespaceOnly(fromScheme)) {
            if (isLocal) {
                // file for local
                fromScheme = "file";
            } else {
                // use default values from fs.default.name
                URI defaultURI = FileSystem.get(conf).getUri();
                fromScheme = defaultURI.getScheme();
                fromAuthority = defaultURI.getAuthority();
            }
        }

        // if scheme is specified but not authority then use the default authority
        if ((!fromScheme.equals("file")) && StringUtils.isNullOrWhitespaceOnly(fromAuthority)) {
            URI defaultURI = FileSystem.get(conf).getUri();
            fromAuthority = defaultURI.getAuthority();
        }

        return new URI(fromScheme, fromAuthority, path, null, null);
    }

    private void ensureFileFormatsMatch(
            TableSpec ts, List<FileStatus> fileStatuses, final URI fromURI)
            throws SemanticException {
        final Class<? extends InputFormat> destInputFormat;
        try {
            if (ts.getPartSpec() == null || ts.getPartSpec().isEmpty()) {
                destInputFormat = ts.tableHandle.getInputFormatClass();
            } else {
                destInputFormat = ts.partHandle.getInputFormatClass();
            }
        } catch (HiveException e) {
            throw new SemanticException(e);
        }

        try {
            FileSystem fs = FileSystem.get(fromURI, conf);
            boolean validFormat =
                    HiveFileFormatUtils.checkInputFormat(fs, conf, destInputFormat, fileStatuses);
            if (!validFormat) {
                throw new SemanticException(ErrorMsg.INVALID_FILE_FORMAT_IN_LOAD.getMsg());
            }
        } catch (Exception e) {
            throw new SemanticException(
                    "Unable to load data to destination table." + " Error: " + e.getMessage());
        }
    }

    // Rewrite the "LOAD DATA" to "INSERT AS SELECT"
    private HiveLoadDataOperation rewriteLoadDataSQL(
            HiveCatalog hiveCatalog,
            Table table,
            URI fromURI,
            HiveParserASTNode tableTree,
            boolean isOverWrite)
            throws SemanticException {
        // Step 1 : Create a temp table object
        // Create a Table object
        Table tempTableObj =
                new Table(new org.apache.hadoop.hive.metastore.api.Table(table.getTTable()));

        // Construct a temp table name
        String tempTblName = table.getTableName() + TEMP_TABLE_FOR_LOAD_DATA_NAME_SUFFIX;
        tempTableObj.setTableName(tempTblName);

        // Reset table params
        tempTableObj.setParameters(new HashMap<>());

        // Set data location and input format, it must be text
        tempTableObj.setDataLocation(new Path(fromURI));
        if (inputFormatClassName != null && serDeClassName != null) {
            try {
                tempTableObj.setInputFormatClass(inputFormatClassName);
                tempTableObj.setSerializationLib(serDeClassName);
            } catch (HiveException e) {
                throw new SemanticException("Load Data: Failed to set inputFormat or SerDe");
            }
        }

        // Make the columns list for the temp table (input data file).
        // Move all the partition columns at the end of table columns.
        ArrayList<FieldSchema> colList = new ArrayList<>(table.getCols());

        // inpPartSpec is a mapping from partition column name to its value.
        Map<String, String> inpPartSpec = null;

        // Partition spec was already validated by caller when create TableSpec object.
        // So, need not validate inpPartSpec here.
        List<FieldSchema> parts = table.getPartCols();
        if (tableTree.getChildCount() >= 2) {
            ASTNode partSpecNode = (ASTNode) tableTree.getChild(1);
            inpPartSpec = new HashMap<>(partSpecNode.getChildCount());

            for (int i = 0; i < partSpecNode.getChildCount(); ++i) {
                ASTNode partSpecValNode = (ASTNode) partSpecNode.getChild(i);
                String partVal = null;
                String partColName =
                        unescapeIdentifier(partSpecValNode.getChild(0).getText().toLowerCase());

                if (partSpecValNode.getChildCount()
                        >= 2) { // in the form of T partition (ds="2010-03-03")
                    // Not stripping quotes here as we need to use it as it is while framing
                    // PARTITION clause
                    // in INSERT query.
                    partVal = partSpecValNode.getChild(1).getText();
                }
                inpPartSpec.put(partColName, partVal);
            }

            // Add only dynamic partition columns to the temp table (input data file).
            // For static partitions, values would be obtained from partition(key=value...) clause.
            for (FieldSchema fs : parts) {
                String partKey = fs.getName();

                // If a partition value is not there, then it is dynamic partition key.
                if (inpPartSpec.get(partKey) == null) {
                    colList.add(fs);
                }
            }
        } else {
            // No static partitions specified and hence all are dynamic partition keys and need to
            // be part
            // of temp table (input data file).
            colList.addAll(parts);
        }

        // Set columns list for temp table.
        tempTableObj.setFields(colList);

        // Wipe out partition columns
        tempTableObj.setPartCols(new ArrayList<>());

        // put the temporary table into
        hiveCatalog.putTemporaryTable(tempTableObj.getTTable());

        // Step 2 : create the Insert query
        StringBuilder rewrittenQueryStr = new StringBuilder();

        if (isOverWrite) {
            rewrittenQueryStr.append("insert overwrite table ");
        } else {
            rewrittenQueryStr.append("insert into table ");
        }

        rewrittenQueryStr.append(
                getDotName(getQualifiedTableName((HiveParserASTNode) tableTree.getChild(0))));
        addPartitionColsToInsert(table.getPartCols(), inpPartSpec, rewrittenQueryStr);
        rewrittenQueryStr.append(" select * from ");
        rewrittenQueryStr.append(tempTblName);
        return new HiveLoadDataOperation(true, rewrittenQueryStr.toString(), tempTableObj);
    }

    /**
     * Append list of partition columns to Insert statement. If user specified partition spec, then
     * use it to get/set the value for partition column else use dynamic partition mode with no
     * value. Static partition mode: INSERT INTO T PARTITION(partCol1=val1,partCol2...) SELECT col1,
     * ... partCol1,partCol2... Dynamic partition mode: INSERT INTO T
     * PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
     */
    protected void addPartitionColsToInsert(
            List<FieldSchema> partCols,
            Map<String, String> partSpec,
            StringBuilder rewrittenQueryStr) {
        // If the table is partitioned we have to put the partition() clause in
        if (partCols != null && partCols.size() > 0) {
            rewrittenQueryStr.append(" partition (");
            boolean first = true;
            for (FieldSchema fschema : partCols) {
                if (first) {
                    first = false;
                } else {
                    rewrittenQueryStr.append(", ");
                }
                // Would be nice if there was a way to determine if quotes are needed
                rewrittenQueryStr.append(HiveUtils.unparseIdentifier(fschema.getName(), this.conf));
                String partVal = (partSpec != null) ? partSpec.get(fschema.getName()) : null;
                if (partVal != null) {
                    rewrittenQueryStr.append("=").append(partVal);
                }
            }
            rewrittenQueryStr.append(")");
        }
    }
}
