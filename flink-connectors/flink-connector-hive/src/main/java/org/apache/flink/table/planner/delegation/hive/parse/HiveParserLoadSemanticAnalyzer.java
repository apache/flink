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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.CatalogRegistry;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.TableSpec;
import org.apache.flink.table.planner.delegation.hive.operations.HiveExecutableOperation;
import org.apache.flink.table.planner.delegation.hive.operations.HiveLoadDataOperation;
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
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.mapred.InputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.stripQuotes;

/** Ported hive's org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer. */
public class HiveParserLoadSemanticAnalyzer {

    private final HiveConf conf;
    private final Hive db;
    private final FrameworkConfig frameworkConfig;
    private final RelOptCluster cluster;
    private final CatalogRegistry catalogRegistry;

    public HiveParserLoadSemanticAnalyzer(
            HiveConf conf,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster,
            CatalogRegistry catalogRegistry)
            throws SemanticException {
        this.conf = conf;
        try {
            this.db = Hive.get(conf);
        } catch (HiveException e) {
            throw new SemanticException(e);
        }
        this.frameworkConfig = frameworkConfig;
        this.cluster = cluster;
        this.catalogRegistry = catalogRegistry;
    }

    public Operation convertToOperation(HiveParserASTNode ast) throws SemanticException {
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
        TableSpec ts = new TableSpec(catalogRegistry, conf, tableTree, frameworkConfig, cluster);
        if (!HiveCatalog.isHiveTable(ts.table.getOptions())) {
            throw new UnsupportedOperationException(
                    "Load data into non-hive table is not supported yet.");
        }
        if (!ts.tableIdentifier.getCatalogName().equals(catalogRegistry.getCurrentCatalog())) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Load data into a table which isn't in current catalog is not supported yet."
                                    + " The table's catalog is %s, but the current catalog is %s.",
                            ts.tableIdentifier.getCatalogName(),
                            catalogRegistry.getCurrentCatalog()));
        }
        Table table;
        try {
            table =
                    db.getTable(
                            ts.tableIdentifier.getDatabaseName(),
                            ts.tableIdentifier.getObjectName());
        } catch (HiveException e) {
            throw new FlinkHiveException(
                    String.format("Fail to get table %s.", ts.tableIdentifier.asSummaryString()),
                    e);
        }

        if (table.isView() || table.isMaterializedView()) {
            throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
        }
        if (table.isNonNative()) {
            throw new SemanticException(ErrorMsg.LOAD_INTO_NON_NATIVE.getMsg());
        }

        if (table.isStoredAsSubDirectories()) {
            throw new SemanticException(ErrorMsg.LOAD_INTO_STORED_AS_DIR.getMsg());
        }

        List<FieldSchema> parts = table.getPartitionKeys();
        if ((parts != null && parts.size() > 0)
                && (ts.partSpec == null || ts.partSpec.size() == 0)) {
            throw new SemanticException(ErrorMsg.NEED_PARTITION_ERROR.getMsg());
        }

        List<String> bucketCols = table.getBucketCols();
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

        // for managed tables, make sure the file formats match
        if (TableType.MANAGED_TABLE.equals(table.getTableType())
                && conf.getBoolVar(HiveConf.ConfVars.HIVECHECKFILEFORMAT)) {
            ensureFileFormatsMatch(ts, table, files, fromURI);
        }

        HiveLoadDataOperation hiveLoadDataOperation =
                new HiveLoadDataOperation(
                        new Path(fromURI),
                        new ObjectPath(table.getDbName(), table.getTableName()),
                        isOverWrite,
                        isLocal,
                        ts.partSpec == null ? new LinkedHashMap<>() : ts.partSpec);
        return new HiveExecutableOperation(hiveLoadDataOperation);
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
                    throw new SemanticException(
                            ErrorMsg.INVALID_PATH.getMsg(
                                    ast,
                                    "source contains directory: " + oneSrc.getPath().toString()));
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
            TableSpec ts, Table table, List<FileStatus> fileStatuses, final URI fromURI)
            throws SemanticException {
        final Class<? extends InputFormat> destInputFormat;
        try {
            if (ts.getPartSpec() == null || ts.getPartSpec().isEmpty()) {
                destInputFormat = table.getInputFormatClass();
            } else {
                Partition partition = db.getPartition(table, ts.partSpec, false);
                if (partition == null) {
                    partition = new Partition(table, ts.partSpec, null);
                }
                destInputFormat = partition.getInputFormatClass();
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
}
