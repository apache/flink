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

import org.apache.flink.table.catalog.ObjectPath;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.common.util.HiveStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

/** Utils for Hive's SHOW TABLE statement. */
public class HiveShowTableUtils {

    // the default serialization format.
    // In Hive2, the value is
    // org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_SERIALIZATION_FORMAT
    // In Hive3, the value is org.apache.hadoop.hive.metastore.DEFAULT_SERIALIZATION_FORMAT.
    // Both of them are "1", but exist in different class.
    // so we port the value to here
    private static final String DEFAULT_SERIALIZATION_FORMAT = "1";

    /** Construct the string for SHOW CREATE TABLE statement. Most of the logic is from Hive's. */
    public static String showCreateTable(ObjectPath tablePath, Table tbl) {
        boolean needsLocation;
        String showCreateTableString;
        List<String> duplicateProps = new ArrayList<>();
        needsLocation = doesTableNeedLocation(tbl);
        if (tbl.isView()) {
            showCreateTableString =
                    "CREATE VIEW `" + tablePath + "` AS " + tbl.getViewExpandedText();
        } else {
            StringBuilder createTabStringBuilder = new StringBuilder();
            // For cases where the table is temporary
            String tblTemp = "";
            if (tbl.isTemporary()) {
                duplicateProps.add("TEMPORARY");
                tblTemp = "TEMPORARY ";
            }
            // For cases where the table is external
            String tblExternal = "";
            if (tbl.getTableType() == TableType.EXTERNAL_TABLE) {
                duplicateProps.add("EXTERNAL");
                tblExternal = "EXTERNAL ";
            }

            createTabStringBuilder.append(
                    String.format(
                            "CREATE %s%sTABLE `%s`",
                            tblTemp, tblExternal, tablePath.getFullName()));

            // Columns
            String tblColumns;
            List<FieldSchema> cols = tbl.getCols();
            List<String> columns = new ArrayList<>();
            for (FieldSchema col : cols) {
                String columnDesc = "  `" + col.getName() + "` " + col.getType();
                if (col.getComment() != null) {
                    columnDesc =
                            columnDesc
                                    + " COMMENT '"
                                    + HiveStringUtils.escapeHiveCommand(col.getComment())
                                    + "'";
                }
                columns.add(columnDesc);
            }
            tblColumns = StringUtils.join(columns, ", \n");
            createTabStringBuilder.append(String.format("(\n%s)\n", tblColumns));

            // Table comment
            String tblComment;
            String tabComment = tbl.getProperty("comment");
            if (tabComment != null) {
                duplicateProps.add("comment");
                tblComment = "COMMENT '" + HiveStringUtils.escapeHiveCommand(tabComment) + "'";
                createTabStringBuilder.append(String.format("%s\n", tblComment));
            }

            // Partitions
            String tblPartitions = "";
            List<FieldSchema> partKeys = tbl.getPartitionKeys();
            if (partKeys.size() > 0) {
                tblPartitions += "PARTITIONED BY ( \n";
                List<String> partCols = new ArrayList<>();
                for (FieldSchema partKey : partKeys) {
                    String partColDesc = "  `" + partKey.getName() + "` " + partKey.getType();
                    if (partKey.getComment() != null) {
                        partColDesc =
                                partColDesc
                                        + " COMMENT '"
                                        + HiveStringUtils.escapeHiveCommand(partKey.getComment())
                                        + "'";
                    }
                    partCols.add(partColDesc);
                }
                tblPartitions += StringUtils.join(partCols, ", \n");
                tblPartitions += ")";
            }
            if (!tblPartitions.equals("")) {
                createTabStringBuilder.append(String.format("%s\n", tblPartitions));
            }

            // Clusters (Buckets)
            String tblSortBucket = "";
            List<String> buckCols = tbl.getBucketCols();
            if (buckCols.size() > 0) {
                duplicateProps.add("SORTBUCKETCOLSPREFIX");
                tblSortBucket += "CLUSTERED BY ( \n  ";
                tblSortBucket += StringUtils.join(buckCols, ", \n  ");
                tblSortBucket += ") \n";
                List<Order> sortCols = tbl.getSortCols();
                if (sortCols.size() > 0) {
                    tblSortBucket += "SORTED BY ( \n";
                    // Order
                    List<String> sortKeys = new ArrayList<String>();
                    for (Order sortCol : sortCols) {
                        String sortKeyDesc = "  " + sortCol.getCol() + " ";
                        if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) {
                            sortKeyDesc = sortKeyDesc + "ASC";
                        } else if (sortCol.getOrder()
                                == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_DESC) {
                            sortKeyDesc = sortKeyDesc + "DESC";
                        }
                        sortKeys.add(sortKeyDesc);
                    }
                    tblSortBucket += StringUtils.join(sortKeys, ", \n");
                    tblSortBucket += ") \n";
                }
                tblSortBucket += "INTO " + tbl.getNumBuckets() + " BUCKETS";
                createTabStringBuilder.append(String.format("%s\n", tblSortBucket));
            }

            // Skewed Info
            StringBuilder tblSkewedInfo = new StringBuilder();
            SkewedInfo skewedInfo = tbl.getSkewedInfo();
            if (skewedInfo != null && !skewedInfo.getSkewedColNames().isEmpty()) {
                tblSkewedInfo
                        .append("SKEWED BY (")
                        .append(StringUtils.join(skewedInfo.getSkewedColNames(), ","))
                        .append(")\n");
                tblSkewedInfo.append("  ON (");
                List<String> colValueList = new ArrayList<>();
                for (List<String> colValues : skewedInfo.getSkewedColValues()) {
                    colValueList.add("('" + StringUtils.join(colValues, "','") + "')");
                }
                tblSkewedInfo.append(StringUtils.join(colValueList, ",")).append(")");
                if (tbl.isStoredAsSubDirectories()) {
                    tblSkewedInfo.append("\n  STORED AS DIRECTORIES");
                }
                createTabStringBuilder.append(String.format("%s\n", tblSkewedInfo));
            }

            // Row format (SerDe)
            StringBuilder tblRowFormat = new StringBuilder();
            StorageDescriptor sd = tbl.getTTable().getSd();
            SerDeInfo serdeInfo = sd.getSerdeInfo();
            Map<String, String> serdeParams = serdeInfo.getParameters();
            tblRowFormat.append("ROW FORMAT SERDE \n");
            tblRowFormat
                    .append("  '")
                    .append(HiveStringUtils.escapeHiveCommand(serdeInfo.getSerializationLib()))
                    .append("' \n");
            if (tbl.getStorageHandler() == null) {
                // If serialization.format property has the default value, it will not to be
                // included in SERDE properties
                if (DEFAULT_SERIALIZATION_FORMAT.equals(
                        serdeParams.get(serdeConstants.SERIALIZATION_FORMAT))) {
                    serdeParams.remove(serdeConstants.SERIALIZATION_FORMAT);
                }
                if (!serdeParams.isEmpty()) {
                    appendSerdeParams(tblRowFormat, serdeParams).append(" \n");
                }
                tblRowFormat
                        .append("STORED AS INPUTFORMAT \n  '")
                        .append(HiveStringUtils.escapeHiveCommand(sd.getInputFormat()))
                        .append("' \n");
                tblRowFormat
                        .append("OUTPUTFORMAT \n  '")
                        .append(HiveStringUtils.escapeHiveCommand(sd.getOutputFormat()))
                        .append("'");
            } else {
                duplicateProps.add(META_TABLE_STORAGE);
                tblRowFormat
                        .append("STORED BY \n  '")
                        .append(
                                HiveStringUtils.escapeHiveCommand(
                                        tbl.getParameters().get(META_TABLE_STORAGE)))
                        .append("' \n");
                // SerDe Properties
                if (!serdeParams.isEmpty()) {
                    appendSerdeParams(tblRowFormat, serdeInfo.getParameters());
                }
            }
            createTabStringBuilder.append(String.format("%s\n", tblRowFormat));

            // table location
            if (needsLocation) {
                String tblLocation =
                        "  '" + HiveStringUtils.escapeHiveCommand(sd.getLocation()) + "'";
                createTabStringBuilder.append(String.format("LOCATION\n%s\n", tblLocation));
            }

            // Table properties
            duplicateProps.addAll(
                    Arrays.stream(StatsSetupConst.TABLE_PARAMS_STATS_KEYS)
                            .collect(Collectors.toList()));
            String tblProperties = propertiesToString(tbl.getParameters(), duplicateProps);
            createTabStringBuilder.append(String.format("TBLPROPERTIES (\n%s)\n", tblProperties));
            showCreateTableString = createTabStringBuilder.toString();
        }
        return showCreateTableString;
    }

    private static boolean doesTableNeedLocation(Table tbl) {
        boolean retval = true;
        if (tbl.getStorageHandler() != null) {
            String sh = tbl.getStorageHandler().toString();
            retval =
                    !sh.equals("org.apache.hadoop.hive.hbase.HBaseStorageHandler")
                            && !sh.equals(Constants.DRUID_HIVE_STORAGE_HANDLER_ID);
        }
        return retval;
    }

    private static String propertiesToString(Map<String, String> props, List<String> exclude) {
        String propString = "";
        if (!props.isEmpty()) {
            Map<String, String> properties = new TreeMap<String, String>(props);
            List<String> realProps = new ArrayList<String>();
            for (String key : properties.keySet()) {
                if (properties.get(key) != null && (exclude == null || !exclude.contains(key))) {
                    realProps.add(
                            "  '"
                                    + key
                                    + "'='"
                                    + HiveStringUtils.escapeHiveCommand(properties.get(key))
                                    + "'");
                }
            }
            propString += StringUtils.join(realProps, ", \n");
        }
        return propString;
    }

    private static StringBuilder appendSerdeParams(
            StringBuilder builder, Map<String, String> serdeParam) {
        serdeParam = new TreeMap<>(serdeParam);
        builder.append("WITH SERDEPROPERTIES ( \n");
        List<String> serdeCols = new ArrayList<>();
        for (Map.Entry<String, String> entry : serdeParam.entrySet()) {
            serdeCols.add(
                    "  '"
                            + entry.getKey()
                            + "'='"
                            + HiveStringUtils.escapeHiveCommand(entry.getValue())
                            + "'");
        }
        builder.append(StringUtils.join(serdeCols, ", \n")).append(')');
        return builder;
    }
}
