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

import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}
import org.apache.calcite.sql.`type`._

/**
  * All built-in scalar SQL functions.
  */
object ScalarSqlFunctions {

  val E = new SqlFunction(
    "E",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE,
    null,
    OperandTypes.NILADIC,
    SqlFunctionCategory.NUMERIC)

  val TANH = new SqlFunction(
    "TANH",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.NUMERIC,
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

  val CONCAT = new SqlFunction(
    "CONCAT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.ONE_OR_MORE,
    SqlFunctionCategory.STRING)

  val CONCAT_WS = new SqlFunction(
    "CONCAT_WS",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    null,
    OperandTypes.ONE_OR_MORE,
    SqlFunctionCategory.STRING)

  val LOG = new SqlFunction(
    "LOG",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.or(OperandTypes.NUMERIC,
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)),
    SqlFunctionCategory.NUMERIC)

  val LOG2 = new SqlFunction(
    "LOG2",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC
  )

  val COSH = new SqlFunction(
    "COSH",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DOUBLE_NULLABLE,
    null,
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC)

  val LPAD = new SqlFunction(
    "LPAD",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(
      ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.FORCE_NULLABLE),
    null,
    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER),
    SqlFunctionCategory.STRING)

  val RPAD = new SqlFunction(
    "RPAD",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(
      ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.FORCE_NULLABLE),
    null,
    OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER),
    SqlFunctionCategory.STRING)

  val MD5 = new SqlFunction(
    "MD5",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING
  )

  val SHA1 = new SqlFunction(
    "SHA1",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING
  )

  val SHA224 = new SqlFunction(
    "SHA224",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING
  )

  val SHA256 = new SqlFunction(
    "SHA256",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING
  )

  val SHA384 = new SqlFunction(
    "SHA384",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING
  )

  val SHA512 = new SqlFunction(
    "SHA512",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING
  )

  val SHA2 = new SqlFunction(
    "SHA2",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.sequence("'(DATA, HASH_LENGTH)'",
      OperandTypes.STRING,  OperandTypes.NUMERIC_INTEGER),
    SqlFunctionCategory.STRING
  )

  val UUID: SqlFunction = new SqlFunction(
    "UUID",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.VARCHAR_2000,
    null,
    OperandTypes.NILADIC,
    SqlFunctionCategory.STRING
  ) {
    override def isDeterministic: Boolean = false
  }

  val DATE_FORMAT = new SqlFunction(
    "DATE_FORMAT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE,
    InferTypes.RETURN_TYPE,
    OperandTypes.sequence("'(TIMESTAMP, FORMAT)'", OperandTypes.DATETIME, OperandTypes.STRING),
    SqlFunctionCategory.TIMEDATE
  )

  val REGEXP_REPLACE = new SqlFunction(
    "REGEXP_REPLACE",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(
      ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING_STRING_STRING,
    SqlFunctionCategory.STRING
  )

  val REGEXP_EXTRACT = new SqlFunction(
    "REGEXP_EXTRACT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(
      ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.FORCE_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.or(OperandTypes.STRING_STRING_INTEGER,
      OperandTypes.STRING_STRING
    ),
    SqlFunctionCategory.STRING
  )

  val FROM_BASE64 = new SqlFunction(
    "FROM_BASE64",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(
      ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.family(SqlTypeFamily.STRING),
    SqlFunctionCategory.STRING
  )

  val TO_BASE64 = new SqlFunction(
    "TO_BASE64",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING
  )

  val LTRIM = new SqlFunction(
    "LTRIM",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING)

  val RTRIM = new SqlFunction(
    "RTRIM",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.STRING,
    SqlFunctionCategory.STRING)

  val REPEAT = new SqlFunction(
    "REPEAT",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), SqlTypeTransforms.TO_NULLABLE),
    InferTypes.RETURN_TYPE,
    OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
    SqlFunctionCategory.STRING)

}
