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

package org.apache.flink.table.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Operator table that contains only Flink-specific functions and operators.
 */
public class FlinkSqlOperatorTable extends ReflectiveSqlOperatorTable {

	/**
	 * The table of contains Flink-specific operators.
	 */
	private static FlinkSqlOperatorTable instance;

	/**
	 * Returns the Flink operator table, creating it if necessary.
	 */
	public static synchronized FlinkSqlOperatorTable instance() {
		if (instance == null) {
			// Creates and initializes the standard operator table.
			// Uses two-phase construction, because we can't initialize the
			// table until the constructor of the sub-class has completed.
			instance = new FlinkSqlOperatorTable();
			instance.init();
		}
		return instance;
	}

	// -----------------------------------------------------------------------------

	private static final SqlReturnTypeInference FLINK_QUOTIENT_NULLABLE = new SqlReturnTypeInference() {
		@Override
		public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
			RelDataType type1 = opBinding.getOperandType(0);
			RelDataType type2 = opBinding.getOperandType(1);
			if (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2)) {
				return ReturnTypes.QUOTIENT_NULLABLE.inferReturnType(opBinding);
			} else {
				RelDataType doubleType = opBinding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
				if (type1.isNullable() || type2.isNullable()) {
					return opBinding.getTypeFactory().createTypeWithNullability(doubleType, true);
				} else {
					return doubleType;
				}
			}
		}
	};

	// -----------------------------------------------------------------------------
	// Flink specific built-in scalar SQL functions
	// -----------------------------------------------------------------------------

	/**
	 * Arithmetic division operator, '/'.
	 */
	public static final SqlBinaryOperator DIVIDE = new SqlBinaryOperator(
		"/",
		SqlKind.DIVIDE,
		60,
		true,
		FLINK_QUOTIENT_NULLABLE,
		InferTypes.FIRST_KNOWN,
		OperandTypes.DIVISION_OPERATOR);

	/** Function used to access a processing time attribute. */
	public static final SqlFunction PROCTIME = new ProctimeSqlFunction();

	/** Function used to materialize a processing time attribute. */
	public static final SqlFunction PROCTIME_MATERIALIZE = new ProctimeMaterializeSqlFunction();

	/** Function to access the timestamp of a StreamRecord. */
	public static final SqlFunction STREAMRECORD_TIMESTAMP = new StreamRecordTimestampSqlFunction();

	// -----------------------------------------------------------------------------
	// Window SQL functions
	// -----------------------------------------------------------------------------

	// TODO: add window functions here

	// -----------------------------------------------------------------------------
	// operators extend from Calcite
	// -----------------------------------------------------------------------------

	// SET OPERATORS
	public static final SqlOperator UNION = SqlStdOperatorTable.UNION;
	public static final SqlOperator UNION_ALL = SqlStdOperatorTable.UNION_ALL;
	public static final SqlOperator EXCEPT = SqlStdOperatorTable.EXCEPT;
	public static final SqlOperator EXCEPT_ALL = SqlStdOperatorTable.EXCEPT_ALL;
	public static final SqlOperator INTERSECT = SqlStdOperatorTable.INTERSECT;
	public static final SqlOperator INTERSECT_ALL = SqlStdOperatorTable.INTERSECT_ALL;

	// BINARY OPERATORS
	public static final SqlOperator AND = SqlStdOperatorTable.AND;
	public static final SqlOperator AS = SqlStdOperatorTable.AS;
	public static final SqlOperator CONCAT = SqlStdOperatorTable.CONCAT;
	public static final SqlOperator DIVIDE_INTEGER = SqlStdOperatorTable.DIVIDE_INTEGER;
	public static final SqlOperator DOT = SqlStdOperatorTable.DOT;
	public static final SqlOperator EQUALS = SqlStdOperatorTable.EQUALS;
	public static final SqlOperator GREATER_THAN = SqlStdOperatorTable.GREATER_THAN;
	public static final SqlOperator IS_DISTINCT_FROM = SqlStdOperatorTable.IS_DISTINCT_FROM;
	public static final SqlOperator IS_NOT_DISTINCT_FROM = SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
	public static final SqlOperator GREATER_THAN_OR_EQUAL = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
	public static final SqlOperator LESS_THAN = SqlStdOperatorTable.LESS_THAN;
	public static final SqlOperator LESS_THAN_OR_EQUAL = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
	public static final SqlOperator MINUS = SqlStdOperatorTable.MINUS;
	public static final SqlOperator MINUS_DATE = SqlStdOperatorTable.MINUS_DATE;
	public static final SqlOperator MULTIPLY = SqlStdOperatorTable.MULTIPLY;
	public static final SqlOperator NOT_EQUALS = SqlStdOperatorTable.NOT_EQUALS;
	public static final SqlOperator OR = SqlStdOperatorTable.OR;
	public static final SqlOperator PLUS = SqlStdOperatorTable.PLUS;
	public static final SqlOperator DATETIME_PLUS = SqlStdOperatorTable.DATETIME_PLUS;

	// POSTFIX OPERATORS
	public static final SqlOperator DESC = SqlStdOperatorTable.DESC;
	public static final SqlOperator NULLS_FIRST = SqlStdOperatorTable.NULLS_FIRST;
	public static final SqlOperator NULLS_LAST = SqlStdOperatorTable.NULLS_LAST;
	public static final SqlOperator IS_NOT_NULL = SqlStdOperatorTable.IS_NOT_NULL;
	public static final SqlOperator IS_NULL = SqlStdOperatorTable.IS_NULL;
	public static final SqlOperator IS_NOT_TRUE = SqlStdOperatorTable.IS_NOT_TRUE;
	public static final SqlOperator IS_TRUE = SqlStdOperatorTable.IS_TRUE;
	public static final SqlOperator IS_NOT_FALSE = SqlStdOperatorTable.IS_NOT_FALSE;
	public static final SqlOperator IS_FALSE = SqlStdOperatorTable.IS_FALSE;
	public static final SqlOperator IS_NOT_UNKNOWN = SqlStdOperatorTable.IS_NOT_UNKNOWN;
	public static final SqlOperator IS_UNKNOWN = SqlStdOperatorTable.IS_UNKNOWN;

	// PREFIX OPERATORS
	public static final SqlOperator NOT = SqlStdOperatorTable.NOT;
	public static final SqlOperator UNARY_MINUS = SqlStdOperatorTable.UNARY_MINUS;
	public static final SqlOperator UNARY_PLUS = SqlStdOperatorTable.UNARY_PLUS;

	// GROUPING FUNCTIONS
	public static final SqlFunction GROUP_ID = SqlStdOperatorTable.GROUP_ID;
	public static final SqlFunction GROUPING = SqlStdOperatorTable.GROUPING;
	public static final SqlFunction GROUPING_ID = SqlStdOperatorTable.GROUPING_ID;

	// AGGREGATE OPERATORS
	public static final SqlAggFunction SUM = SqlStdOperatorTable.SUM;
	public static final SqlAggFunction SUM0 = SqlStdOperatorTable.SUM0;
	public static final SqlAggFunction COUNT = SqlStdOperatorTable.COUNT;
	public static final SqlAggFunction APPROX_COUNT_DISTINCT = SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
	public static final SqlAggFunction COLLECT = SqlStdOperatorTable.COLLECT;
	public static final SqlAggFunction MIN = SqlStdOperatorTable.MIN;
	public static final SqlAggFunction MAX = SqlStdOperatorTable.MAX;
	public static final SqlAggFunction AVG = SqlStdOperatorTable.AVG;
	public static final SqlAggFunction STDDEV = SqlStdOperatorTable.STDDEV;
	public static final SqlAggFunction STDDEV_POP = SqlStdOperatorTable.STDDEV_POP;
	public static final SqlAggFunction STDDEV_SAMP = SqlStdOperatorTable.STDDEV_SAMP;
	public static final SqlAggFunction VARIANCE = SqlStdOperatorTable.VARIANCE;
	public static final SqlAggFunction VAR_POP = SqlStdOperatorTable.VAR_POP;
	public static final SqlAggFunction VAR_SAMP = SqlStdOperatorTable.VAR_SAMP;

	// ARRAY OPERATORS
	public static final SqlOperator ARRAY_VALUE_CONSTRUCTOR = SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
	public static final SqlOperator ELEMENT = SqlStdOperatorTable.ELEMENT;

	// MAP OPERATORS
	public static final SqlOperator MAP_VALUE_CONSTRUCTOR = SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;

	// ARRAY MAP SHARED OPERATORS
	public static final SqlOperator ITEM = SqlStdOperatorTable.ITEM;
	public static final SqlOperator CARDINALITY = SqlStdOperatorTable.CARDINALITY;

	// SPECIAL OPERATORS
	public static final SqlOperator MULTISET_VALUE = SqlStdOperatorTable.MULTISET_VALUE;
	public static final SqlOperator ROW = SqlStdOperatorTable.ROW;
	public static final SqlOperator OVERLAPS = SqlStdOperatorTable.OVERLAPS;
	public static final SqlOperator LITERAL_CHAIN = SqlStdOperatorTable.LITERAL_CHAIN;
	public static final SqlOperator BETWEEN = SqlStdOperatorTable.BETWEEN;
	public static final SqlOperator SYMMETRIC_BETWEEN = SqlStdOperatorTable.SYMMETRIC_BETWEEN;
	public static final SqlOperator NOT_BETWEEN = SqlStdOperatorTable.NOT_BETWEEN;
	public static final SqlOperator SYMMETRIC_NOT_BETWEEN = SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN;
	public static final SqlOperator NOT_LIKE = SqlStdOperatorTable.NOT_LIKE;
	public static final SqlOperator LIKE = SqlStdOperatorTable.LIKE;
	public static final SqlOperator NOT_SIMILAR_TO = SqlStdOperatorTable.NOT_SIMILAR_TO;
	public static final SqlOperator SIMILAR_TO = SqlStdOperatorTable.SIMILAR_TO;
	public static final SqlOperator CASE = SqlStdOperatorTable.CASE;
	public static final SqlOperator REINTERPRET = SqlStdOperatorTable.REINTERPRET;
	public static final SqlOperator EXTRACT = SqlStdOperatorTable.EXTRACT;
	public static final SqlOperator IN = SqlStdOperatorTable.IN;
	public static final SqlOperator NOT_IN = SqlStdOperatorTable.NOT_IN;

	// FUNCTIONS
	public static final SqlFunction OVERLAY = SqlStdOperatorTable.OVERLAY;
	public static final SqlFunction TRIM = SqlStdOperatorTable.TRIM;
	public static final SqlFunction POSITION = SqlStdOperatorTable.POSITION;
	public static final SqlFunction CHAR_LENGTH = SqlStdOperatorTable.CHAR_LENGTH;
	public static final SqlFunction CHARACTER_LENGTH = SqlStdOperatorTable.CHARACTER_LENGTH;
	public static final SqlFunction UPPER = SqlStdOperatorTable.UPPER;
	public static final SqlFunction LOWER = SqlStdOperatorTable.LOWER;
	public static final SqlFunction INITCAP = SqlStdOperatorTable.INITCAP;
	public static final SqlFunction POWER = SqlStdOperatorTable.POWER;
	public static final SqlFunction SQRT = SqlStdOperatorTable.SQRT;
	public static final SqlFunction MOD = SqlStdOperatorTable.MOD;
	public static final SqlFunction LN = SqlStdOperatorTable.LN;
	public static final SqlFunction LOG10 = SqlStdOperatorTable.LOG10;
	public static final SqlFunction ABS = SqlStdOperatorTable.ABS;
	public static final SqlFunction EXP = SqlStdOperatorTable.EXP;
	public static final SqlFunction NULLIF = SqlStdOperatorTable.NULLIF;
	public static final SqlFunction COALESCE = SqlStdOperatorTable.COALESCE;
	public static final SqlFunction FLOOR = SqlStdOperatorTable.FLOOR;
	public static final SqlFunction CEIL = SqlStdOperatorTable.CEIL;
	public static final SqlFunction LOCALTIME = SqlStdOperatorTable.LOCALTIME;
	public static final SqlFunction LOCALTIMESTAMP = SqlStdOperatorTable.LOCALTIMESTAMP;
	public static final SqlFunction CURRENT_TIME = SqlStdOperatorTable.CURRENT_TIME;
	public static final SqlFunction CURRENT_TIMESTAMP = SqlStdOperatorTable.CURRENT_TIMESTAMP;
	public static final SqlFunction CURRENT_DATE = SqlStdOperatorTable.CURRENT_DATE;
	public static final SqlFunction CAST = SqlStdOperatorTable.CAST;
	public static final SqlFunction QUARTER = SqlStdOperatorTable.QUARTER;
	public static final SqlOperator SCALAR_QUERY = SqlStdOperatorTable.SCALAR_QUERY;
	public static final SqlOperator EXISTS = SqlStdOperatorTable.EXISTS;
	public static final SqlFunction SIN = SqlStdOperatorTable.SIN;
	public static final SqlFunction COS = SqlStdOperatorTable.COS;
	public static final SqlFunction TAN = SqlStdOperatorTable.TAN;
	public static final SqlFunction COT = SqlStdOperatorTable.COT;
	public static final SqlFunction ASIN = SqlStdOperatorTable.ASIN;
	public static final SqlFunction ACOS = SqlStdOperatorTable.ACOS;
	public static final SqlFunction ATAN = SqlStdOperatorTable.ATAN;
	public static final SqlFunction ATAN2 = SqlStdOperatorTable.ATAN2;
	public static final SqlFunction DEGREES = SqlStdOperatorTable.DEGREES;
	public static final SqlFunction RADIANS = SqlStdOperatorTable.RADIANS;
	public static final SqlFunction SIGN = SqlStdOperatorTable.SIGN;
	public static final SqlFunction PI = SqlStdOperatorTable.PI;
	public static final SqlFunction RAND = SqlStdOperatorTable.RAND;
	public static final SqlFunction RAND_INTEGER = SqlStdOperatorTable.RAND_INTEGER;
	public static final SqlFunction TIMESTAMP_ADD = SqlStdOperatorTable.TIMESTAMP_ADD;
	public static final SqlFunction TIMESTAMP_DIFF = SqlStdOperatorTable.TIMESTAMP_DIFF;

	// MATCH_RECOGNIZE
	public static final SqlFunction FIRST = SqlStdOperatorTable.FIRST;
	public static final SqlFunction LAST = SqlStdOperatorTable.LAST;
	public static final SqlFunction PREV = SqlStdOperatorTable.PREV;
	public static final SqlFunction NEXT = SqlStdOperatorTable.NEXT;
	public static final SqlFunction CLASSIFIER = SqlStdOperatorTable.CLASSIFIER;
	public static final SqlOperator FINAL = SqlStdOperatorTable.FINAL;
	public static final SqlOperator RUNNING = SqlStdOperatorTable.RUNNING;

	// OVER FUNCTIONS
	public static final SqlAggFunction RANK = SqlStdOperatorTable.RANK;
	public static final SqlAggFunction DENSE_RANK = SqlStdOperatorTable.DENSE_RANK;
	public static final SqlAggFunction ROW_NUMBER = SqlStdOperatorTable.ROW_NUMBER;
	public static final SqlAggFunction LEAD = SqlStdOperatorTable.LEAD;
	public static final SqlAggFunction LAG = SqlStdOperatorTable.LAG;
}
