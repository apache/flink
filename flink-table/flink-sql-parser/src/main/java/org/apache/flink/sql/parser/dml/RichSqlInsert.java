/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.dml;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlProperty;
import org.apache.flink.sql.parser.error.SqlParseException;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlInsertKeyword;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.LinkedHashMap;
import java.util.List;

/** A {@link SqlInsert} that have some extension functions like partition, overwrite. **/
public class RichSqlInsert extends SqlInsert implements ExtendedSqlNode {
	private final SqlNodeList staticPartitions;

	private final SqlNodeList extendedKeywords;

	public RichSqlInsert(SqlParserPos pos,
			SqlNodeList keywords,
			SqlNodeList extendedKeywords,
			SqlNode targetTable,
			SqlNode source,
			SqlNodeList columnList,
			SqlNodeList staticPartitions) {
		super(pos, keywords, targetTable, source, columnList);
		this.extendedKeywords = extendedKeywords;
		this.staticPartitions = staticPartitions;
	}

	/**
	 * @return the list of partition key-value pairs,
	 * returns empty if there is no partition specifications.
	 */
	public SqlNodeList getStaticPartitions() {
		return staticPartitions;
	}

	/** Get static partition key value pair as strings.
	 *
	 * <p>Caution that we use {@link SqlLiteral#toString()} to get
	 * the string format of the value literal. If the string format is not
	 * what you need, use {@link #getStaticPartitions()}.
	 *
	 * @return the mapping of column names to values of partition specifications,
	 * returns an empty map if there is no partition specifications.
	 */
	public LinkedHashMap<String, String> getStaticPartitionKVs() {
		LinkedHashMap<String, String> ret = new LinkedHashMap<>();
		if (this.staticPartitions.size() == 0) {
			return ret;
		}
		for (SqlNode node : this.staticPartitions.getList()) {
			SqlProperty sqlProperty = (SqlProperty) node;
			String value = SqlLiteral.value(sqlProperty.getValue()).toString();
			ret.put(sqlProperty.getKey().getSimple(), value);
		}
		return ret;
	}

	@Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.startList(SqlWriter.FrameTypeEnum.SELECT);
		String insertKeyword = "INSERT INTO";
		if (isUpsert()) {
			insertKeyword = "UPSERT INTO";
		} else if (isOverwrite()) {
			insertKeyword = "INSERT OVERWRITE";
		}
		writer.sep(insertKeyword);
		final int opLeft = getOperator().getLeftPrec();
		final int opRight = getOperator().getRightPrec();
		getTargetTable().unparse(writer, opLeft, opRight);
		if (getTargetColumnList() != null) {
			getTargetColumnList().unparse(writer, opLeft, opRight);
		}
		writer.newlineAndIndent();
		if (staticPartitions != null && staticPartitions.size() > 0) {
			writer.keyword("PARTITION");
			staticPartitions.unparse(writer, opLeft, opRight);
			writer.newlineAndIndent();
		}
		getSource().unparse(writer, 0, 0);
	}

	//~ Tools ------------------------------------------------------------------

	public static boolean isUpsert(List<SqlLiteral> keywords) {
		for (SqlNode keyword : keywords) {
			SqlInsertKeyword keyword2 =
				((SqlLiteral) keyword).symbolValue(SqlInsertKeyword.class);
			if (keyword2 == SqlInsertKeyword.UPSERT) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns whether the insert mode is overwrite (for whole table or for specific partitions).
	 *
	 * @return true if this is overwrite mode
	 */
	public boolean isOverwrite() {
		return getModifierNode(RichSqlInsertKeyword.OVERWRITE) != null;
	}

	private SqlNode getModifierNode(RichSqlInsertKeyword modifier) {
		for (SqlNode keyword : extendedKeywords) {
			RichSqlInsertKeyword keyword2 =
				((SqlLiteral) keyword).symbolValue(RichSqlInsertKeyword.class);
			if (keyword2 == modifier) {
				return keyword;
			}
		}
		return null;
	}

	@Override
	public void validate() throws SqlParseException {
		// no-op
	}
}
