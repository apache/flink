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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.addons.hbase.HBaseTableSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.connectors.hbase.table.HBaseValidator.COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_QUALIFIER_DELIMITER;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_ROW_KEY;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * the basic and general information for using HBase Connector.
 */
public class HBaseTableContext {
	public static HBaseTableContext create(
		DescriptorProperties descriptorProperties, Configuration hbaseClientConf) {
		return new HBaseTableContext(descriptorProperties, hbaseClientConf);
	}

	private final String hTableName;
	private final TableSchema tableSchema;
	private final HBaseTableSchema hBaseTableSchema;
	private final Configuration hbaseClientConf;
	/**
	 * NOTICE: The HBase table's primary key can only contain one column.
	 */
	private final String rowkey;
	private final TypeInformation<?> rowKeyType;
	private final int rowkeyIndex;
	private List<Integer> qualifierIndexes;

	private HBaseTableContext(DescriptorProperties descriptorProperties, Configuration hbaseClientConf) {
		this.hTableName = descriptorProperties.getString(CONNECTOR_HBASE_TABLE_NAME);
		this.tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		this.hbaseClientConf = hbaseClientConf;
		this.rowkey = descriptorProperties.getString(CONNECTOR_ROW_KEY);

		DataType[] typeInformations = this.tableSchema.getFieldDataTypes();

		this.rowkeyIndex = initRowkeyIndex();
		this.rowKeyType = TypeConversions.fromDataTypeToLegacyInfo(typeInformations[this.rowkeyIndex]);
		this.hBaseTableSchema = initHBaseTableSchema(typeInformations, descriptorProperties);
		validateZookeeperQuorum(descriptorProperties, hbaseClientConf);
		Preconditions.checkArgument(null != qualifierIndexes,
			"qualifierSourceIndexes info should be consistent with qualifiers defined in HBaseTableSchema!");
	}

	private void validateZookeeperQuorum(DescriptorProperties properties, Configuration hbaseClientConf) {
		Optional<String> hbaseZk = properties.getOptionalString(HConstants.ZOOKEEPER_QUORUM);
		if (!hbaseZk.isPresent() || StringUtils.isNullOrWhitespaceOnly(hbaseZk.get())) {
			String zkQuorum = hbaseClientConf.get(HConstants.ZOOKEEPER_QUORUM);
			if (StringUtils.isNullOrWhitespaceOnly(zkQuorum)) {
				throw new RuntimeException(HConstants.ZOOKEEPER_QUORUM + " should not be empty! " + "Pls specify it or ensure a default hbase-site.xml is valid in current class path.");
			}
		}
	}

	private HBaseTableSchema initHBaseTableSchema(DataType[] columnTypes, DescriptorProperties descriptorProperties) {
		Preconditions.checkArgument(rowkeyIndex > -1, "must invoke initRokeyIndex first");
		final String delimiter = descriptorProperties.getOptionalString(CONNECTOR_QUALIFIER_DELIMITER).orElse(
			COLUMNFAMILY_QUALIFIER_DELIMITER_PATTERN);

		String[] columnNames = this.tableSchema.getFieldNames();
		HBaseTableSchema hBaseTableSchema = new HBaseTableSchema();
		List<Integer> qualifierSourceIndexes = new ArrayList<>();

		for (int idx = 0; idx < columnNames.length; idx++) {
			if (idx != rowkeyIndex) {
				String[] cfQ = columnNames[idx].split(delimiter);
				Preconditions.checkArgument(2 == cfQ.length,
					"invalid column name'" + columnNames[idx] + "' for HBase qualifier name pattern: `columnFamily.qualifier`!");
				TypeInformation columnType = TypeConversions.fromDataTypeToLegacyInfo(columnTypes[idx]);
				//NOTICE: hBaseTableSchema should keep the order of columns.
				hBaseTableSchema.addColumn(cfQ[0], cfQ[1], columnType);
				qualifierSourceIndexes.add(idx);
			}
		}
		this.qualifierIndexes = qualifierSourceIndexes;
		return hBaseTableSchema;
	}

	private int initRowkeyIndex() {
		String[] columnNames = this.tableSchema.getFieldNames();

		int rowKeySourceIndex = -1;
		for (int idx = 0; idx < columnNames.length; idx++) {
			if (this.rowkey.equals(columnNames[idx])) {
				rowKeySourceIndex = idx;
				break;
			}
		}
		Preconditions.checkArgument(rowKeySourceIndex > -1, "rowKeySourceIndex must > -1, no rowkey?");
		return rowKeySourceIndex;
	}

	public String gethTableName() {
		return hTableName;
	}

	public TableSchema getTableSchema() {
		return this.tableSchema;
	}

	public Configuration getHbaseClientConf() {
		return hbaseClientConf;
	}

	public HBaseTableSchema gethBaseTableSchema() {
		return hBaseTableSchema;
	}

	public String getRowkey() {
		return rowkey;
	}

	public TypeInformation<?> getRowKeyType() {
		return this.rowKeyType;
	}

	public int getRowKeyIndex() {
		return this.rowkeyIndex;
	}

	public List<Integer> getQualifierIndexes() {
		return this.qualifierIndexes;
	}
}
