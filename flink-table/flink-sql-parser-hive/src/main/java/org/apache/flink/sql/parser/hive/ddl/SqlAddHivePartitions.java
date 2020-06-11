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

package org.apache.flink.sql.parser.hive.ddl;

import org.apache.flink.sql.parser.ddl.SqlAddPartitions;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * Add partitions to a Hive table.
 *
 * <p>Hive syntax:
 * ALTER TABLE table_name ADD [IF NOT EXISTS]
 *     PARTITION partition_spec [LOCATION 'location'][PARTITION partition_spec [LOCATION 'location']][...];
 */
public class SqlAddHivePartitions extends SqlAddPartitions {

	private final List<SqlCharStringLiteral> partLocations;

	public SqlAddHivePartitions(SqlParserPos pos, SqlIdentifier tableName, boolean ifNotExists,
			List<SqlNodeList> partSpecs, List<SqlCharStringLiteral> partLocations) {
		super(pos, tableName, ifNotExists, partSpecs, toProps(partLocations));
		for (SqlNodeList spec : partSpecs) {
			HiveDDLUtils.unescapePartitionSpec(spec);
		}
		this.partLocations = partLocations;
	}

	private static List<SqlNodeList> toProps(List<SqlCharStringLiteral> partLocations) {
		List<SqlNodeList> res = new ArrayList<>(partLocations.size());
		for (SqlCharStringLiteral partLocation : partLocations) {
			SqlNodeList prop = null;
			if (partLocation != null) {
				prop = new SqlNodeList(partLocation.getParserPosition());
				prop.add(HiveDDLUtils.toTableOption(SqlCreateHiveTable.TABLE_LOCATION_URI, partLocation, partLocation.getParserPosition()));
			}
			res.add(prop);
		}
		return res;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("ALTER TABLE");
		tableIdentifier.unparse(writer, leftPrec, rightPrec);
		writer.newlineAndIndent();
		writer.keyword("ADD");
		if (ifNotExists()) {
			writer.keyword("IF NOT EXISTS");
		}
		int opLeftPrec = getOperator().getLeftPrec();
		int opRightPrec = getOperator().getRightPrec();
		for (int i = 0; i < getPartSpecs().size(); i++) {
			writer.newlineAndIndent();
			SqlNodeList partSpec = getPartSpecs().get(i);
			writer.keyword("PARTITION");
			partSpec.unparse(writer, opLeftPrec, opRightPrec);
			SqlCharStringLiteral location = partLocations.get(i);
			if (location != null) {
				writer.keyword("LOCATION");
				location.unparse(writer, opLeftPrec, opRightPrec);
			}
		}
	}
}
