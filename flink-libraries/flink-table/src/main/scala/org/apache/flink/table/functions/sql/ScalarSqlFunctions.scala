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
package org.apache.flink.table.functions.sql

import org.apache.flink.table.api.Types
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.calcite.`type`.{FlinkReturnTypes, NumericExceptFirstOperandChecker, RepeatFamilyOperandTypeChecker}
import org.apache.flink.table.dataformat.Decimal

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlTypeFamily, _}
import org.apache.calcite.sql.validate.SqlMonotonicity

/**
  * All built-in scalar SQL functions.
  */
object ScalarSqlFunctions {

  private val VARCHAR_2000_NULLABLE =
    ReturnTypes.cascade(ReturnTypes.VARCHAR_2000, SqlTypeTransforms.FORCE_NULLABLE)

  private val ARG0_VARCHAR_NULLABLE: SqlReturnTypeInference = new OrdinalReturnTypeInference(0) {
    override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
      val `type` = super.inferReturnType(opBinding)
      val newType = `type`.getSqlTypeName match {
        case SqlTypeName.CHAR =>
          opBinding.getTypeFactory.createSqlType(SqlTypeName.VARCHAR, `type`.getPrecision)
        case SqlTypeName.VARCHAR => `type`
        case _ => throw new UnsupportedOperationException(s"Unsupported type:${`type`}")
      }
      opBinding.getTypeFactory.createTypeWithNullability(newType, true)
    }
  }

  private val FLINK_QUOTIENT_NULLABLE = new SqlReturnTypeInference() {
    override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
      val type1 = opBinding.getOperandType(0)
      val type2 = opBinding.getOperandType(1)
      if (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2)) {
        ReturnTypes.QUOTIENT_NULLABLE.inferReturnType(opBinding)
      } else {
        val doubleType = opBinding.getTypeFactory.createSqlType(SqlTypeName.DOUBLE)
        if (type1.isNullable || type2.isNullable) {
          opBinding.getTypeFactory.createTypeWithNullability(doubleType, true)
        } else {
          doubleType
        }
      }
    }
  }

  // see DivCallGen
  private val FLINK_DIV_NULLABLE = new SqlReturnTypeInference() {
    override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
      val type1 = opBinding.getOperandType(0)
      val type2 = opBinding.getOperandType(1)
      val returnType =
        if (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2)) {
          val dt = Decimal.inferIntDivType(
            type1.getPrecision, type1.getScale, type2.getPrecision, type2.getScale)
          opBinding.getTypeFactory.createSqlType(SqlTypeName.DECIMAL, dt.precision, dt.scale)
        } else { // both are primitive
          type1
        }
      opBinding.getTypeFactory.createTypeWithNullability(returnType,
        type1.isNullable || type2.isNullable)
    }
  }

  /**
    * Arithmetic division operator, '/'.
    */
  val DIVIDE = new SqlBinaryOperator(
    "/",
    SqlKind.DIVIDE,
    60,
    true,
    FLINK_QUOTIENT_NULLABLE,
    InferTypes.FIRST_KNOWN,
    OperandTypes.DIVISION_OPERATOR)

  val E = new SqlFunction(
    "E",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.NILADIC,
    SqlFunctionCategory.NUMERIC)

  val PI = new SqlFunction(
    "PI",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.NILADIC,
    SqlFunctionCategory.NUMERIC)

  val CONCAT = new SqlFunction(
    "CONCAT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    null.asInstanceOf[SqlOperandTypeInference],
    new RepeatFamilyOperandTypeChecker(SqlTypeFamily.CHARACTER),
    SqlFunctionCategory.STRING)

  val CONCAT_WS = new SqlFunction(
    "CONCAT_WS",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    null.asInstanceOf[SqlOperandTypeInference],
    new RepeatFamilyOperandTypeChecker(SqlTypeFamily.CHARACTER),
    SqlFunctionCategory.STRING)

  val LOG = new SqlFunction(
    "LOG",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.or(OperandTypes.NUMERIC,
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)),
    SqlFunctionCategory.NUMERIC)

  val ROUND = new SqlFunction(
    "ROUND",
    SqlKind.OTHER_FUNCTION,
    FlinkReturnTypes.ROUND_FUNCTION_NULLABLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.or(OperandTypes.NUMERIC_INTEGER, OperandTypes.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val BIN = new SqlFunction(
    "BIN",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.family(SqlTypeFamily.INTEGER),
    SqlFunctionCategory.NUMERIC)

  val SINH = new SqlFunction(
    "SINH",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC)

  val HEX = new SqlFunction(
    "HEX",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.or(OperandTypes.family(SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING)),
    SqlFunctionCategory.NUMERIC)

  val BITAND = new SqlFunction(
    "BITAND",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val BITOR = new SqlFunction(
    "BITOR",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val BITXOR = new SqlFunction(
    "BITXOR",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val BITNOT = new SqlFunction(
    "BITNOT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC)

  val JSON_VALUE = new SqlFunction(
    "JSON_VALUE",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
    SqlFunctionCategory.STRING)

  val STR_TO_MAP = new SqlFunction(
    "STR_TO_MAP",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.explicit(new RelProtoDataType {
      override def apply(factory: RelDataTypeFactory): RelDataType =
        factory.asInstanceOf[FlinkTypeFactory].createTypeFromTypeInfo(
          Types.MAP(Types.STRING, Types.STRING),
          isNullable = true)
    }),
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)
    ),
    SqlFunctionCategory.STRING)

  val LOG2 = new SqlFunction(
    "LOG2",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC)

  val IS_DECIMAL = new SqlFunction(
    "IS_DECIMAL",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN,
    null.asInstanceOf[SqlOperandTypeInference],
    OperandTypes.or(OperandTypes.CHARACTER, OperandTypes.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val IS_DIGIT = new SqlFunction(
    "IS_DIGIT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN,
    null,
    OperandTypes.or(OperandTypes.CHARACTER, OperandTypes.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val IS_ALPHA = new SqlFunction(
    "IS_ALPHA",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN,
    null,
    OperandTypes.or(OperandTypes.CHARACTER, OperandTypes.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val COSH = new SqlFunction(
    "COSH",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC)

  val TANH = new SqlFunction(
    "TANH",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC)

  val CHR = new SqlFunction(
    "CHR",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.INTEGER),
    SqlFunctionCategory.STRING)

  val LPAD = new SqlFunction(
    "LPAD",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val RPAD = new SqlFunction(
    "RPAD",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val REPEAT = new SqlFunction(
    "REPEAT",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
    SqlFunctionCategory.STRING)

  val REVERSE = new SqlFunction(
    "REVERSE",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val REPLACE = new SqlFunction(
    "REPLACE",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING
  )

  val SPLIT_INDEX = new SqlFunction(
    "SPLIT_INDEX",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)),
    SqlFunctionCategory.STRING)

  val REGEXP_REPLACE = new SqlFunction(
    "REGEXP_REPLACE",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val REGEXP_EXTRACT = new SqlFunction(
    "REGEXP_EXTRACT",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.or(OperandTypes.STRING_STRING_INTEGER,
      OperandTypes.STRING_STRING
    ),
    SqlFunctionCategory.STRING
  )

  val KEYVALUE = new SqlFunction(
    "KEYVALUE",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(
      SqlTypeFamily.STRING,
      SqlTypeFamily.STRING,
      SqlTypeFamily.STRING,
      SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val HASH_CODE = new SqlFunction(
    "HASH_CODE",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.BOOLEAN),
      OperandTypes.family(SqlTypeFamily.NUMERIC),
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.TIMESTAMP),
      OperandTypes.family(SqlTypeFamily.TIME),
      OperandTypes.family(SqlTypeFamily.DATE)),
    SqlFunctionCategory.STRING)

  val MD5 = new SqlFunction(
    "MD5",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val SHA1 = new SqlFunction(
    "SHA1",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val SHA224 = new SqlFunction(
    "SHA224",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val SHA256 = new SqlFunction(
    "SHA256",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val SHA384 = new SqlFunction(
    "SHA384",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val SHA512 = new SqlFunction(
    "SHA512",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val SHA2 = new SqlFunction(
    "SHA2",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)),
    SqlFunctionCategory.STRING)

  val DATE_FORMAT = new SqlFunction(
    "DATE_FORMAT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.or(
      OperandTypes.sequence("'(TIMESTAMP, FORMAT)'",
        OperandTypes.DATETIME, OperandTypes.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE
  )

  val REGEXP = new SqlFunction(
    "REGEXP",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val PARSE_URL = new SqlFunction(
    "PARSE_URL",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val PRINT = new SqlFunction(
    "PRINT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG1_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.ANY),
    SqlFunctionCategory.STRING)

  val NOW = new SqlFunction(
    "NOW",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BIGINT,
    null,
    OperandTypes.or(
      OperandTypes.NILADIC,
      OperandTypes.family(SqlTypeFamily.INTEGER)),
    SqlFunctionCategory.TIMEDATE) {
    override def isDeterministic: Boolean = false

    override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity = SqlMonotonicity
      .INCREASING
  }

  val UNIX_TIMESTAMP = new SqlFunction(
    "UNIX_TIMESTAMP",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BIGINT_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.NILADIC,
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.TIMESTAMP)),
    SqlFunctionCategory.TIMEDATE) {
    override def isDeterministic: Boolean = false

    override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity = SqlMonotonicity
      .INCREASING
  }

  val FROM_UNIXTIME = new SqlFunction(
    "FROM_UNIXTIME",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.NUMERIC),
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE)

  val DATEDIFF = new SqlFunction(
    "DATEDIFF",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.TIMESTAMP),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE)

  val DATE_SUB = new SqlFunction(
    "DATE_SUB",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)),
    SqlFunctionCategory.TIMEDATE)

  val DATE_ADD = new SqlFunction(
    "DATE_ADD",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)),
    SqlFunctionCategory.TIMEDATE)

  val IF = new SqlFunction(
    "IF",
    SqlKind.OTHER_FUNCTION,
    FlinkReturnTypes.NUMERIC_FROM_ARG1_DEFAULT1_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.and(
        // cannot only use `family(BOOLEAN, NUMERIC, NUMERIC)` here,
        // as we don't want non-numeric types to be implicitly casted to numeric types.
        new NumericExceptFirstOperandChecker(3),
        OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
      ),
      OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN),
      OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
      OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BINARY, SqlTypeFamily.BINARY),
      OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.DATE, SqlTypeFamily.DATE),
      OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP),
      OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.TIME, SqlTypeFamily.TIME)
    ),
    SqlFunctionCategory.NUMERIC)

  val DIV = new SqlFunction( // similar to SqlStdOperatorTable.MOD
    "DIV",
    SqlKind.OTHER_FUNCTION,
    FLINK_DIV_NULLABLE,
    null,
    OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC,
    SqlFunctionCategory.NUMERIC)

  val DIV_INT = new SqlFunction(
    "DIV_INT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER_QUOTIENT_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
    SqlFunctionCategory.NUMERIC)

  val TO_BASE64 = new SqlFunction(
    "TO_BASE64",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.ANY,
    SqlFunctionCategory.STRING)

  val FROM_BASE64 = new SqlFunction(
    "FROM_BASE64",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.BINARY), SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.family(SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val UUID = new SqlFunction(
    "UUID",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.VARCHAR_2000,
    null,
    OperandTypes.or(
      OperandTypes.NILADIC,
      OperandTypes.ANY),
    SqlFunctionCategory.STRING) {
    override def isDeterministic: Boolean = false

    // Calcite treat MONOTONIC as CLUSTERED collation(not a mathematic monotonic function)
    override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity =
      SqlMonotonicity.MONOTONIC
  }

  val SUBSTRING = new SqlFunction(
    "SUBSTRING",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
    ),
    SqlFunctionCategory.STRING)

  val SUBSTR = new SqlFunction(
    "SUBSTR",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
    ),
    SqlFunctionCategory.STRING)

  val LEFT = new SqlFunction(
    "LEFT",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
    SqlFunctionCategory.STRING)

  val RIGHT = new SqlFunction(
    "RIGHT",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
    SqlFunctionCategory.STRING)

  val PROCTIME = new SqlFunction(
    "PROCTIME",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.explicit(new RelProtoDataType {
      override def apply(factory: RelDataTypeFactory): RelDataType =
        factory.asInstanceOf[FlinkTypeFactory].createProctimeIndicatorType()
    }),
    null,
    OperandTypes.NILADIC,
    SqlFunctionCategory.TIMEDATE) {

    override def isDeterministic = false

    override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity =
      SqlMonotonicity.INCREASING
  }

  val TO_TIMESTAMP = new SqlFunction(
    "TO_TIMESTAMP",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.TIMESTAMP), SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.NUMERIC),
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE)

  val FROM_TIMESTAMP = new SqlFunction(
    "FROM_TIMESTAMP",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.BIGINT, SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.family(SqlTypeFamily.TIMESTAMP),
    SqlFunctionCategory.TIMEDATE)

  val TO_DATE = new SqlFunction(
    "TO_DATE",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.DATE), SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.NUMERIC),
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE)

  val TO_TIMESTAMP_TZ = new SqlFunction(
    "TO_TIMESTAMP_TZ",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.TIMESTAMP), SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE)

  val DATE_FORMAT_TZ = new SqlFunction(
    "DATE_FORMAT_TZ",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE
  )

  val CONVERT_TZ = new SqlFunction(
    "CONVERT_TZ",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
        SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.TIMEDATE)

  val LOCATE = new SqlFunction(
    "LOCATE",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)),
    SqlFunctionCategory.NUMERIC)

  val LENGTH = new SqlFunction(
    "LENGTH",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING),
    SqlFunctionCategory.NUMERIC)


  val ASCII = new SqlFunction(
    "ASCII",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.STRING),
    SqlFunctionCategory.NUMERIC)

  val ENCODE = new SqlFunction(
    "ENCODE",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.BINARY), SqlTypeTransforms.FORCE_NULLABLE),
    null,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)

  val DECODE = new SqlFunction(
    "DECODE",
    SqlKind.OTHER_FUNCTION,
    VARCHAR_2000_NULLABLE,
    null,
    OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING)


  val INSTR = new SqlFunction(
    "INSTR",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.INTEGER_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,
        SqlTypeFamily.INTEGER)),
    SqlFunctionCategory.NUMERIC)


  val LTRIM = new SqlFunction(
    "LTRIM",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

  val RTRIM = new SqlFunction(
    "RTRIM",
    SqlKind.OTHER_FUNCTION,
    ARG0_VARCHAR_NULLABLE,
    null,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING),
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
    SqlFunctionCategory.STRING)

}
