/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import com.google.common.collect.ImmutableList;

import java.util.List;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT UNTIL CALCITE-1867 IS FIXED.
 */

/**
 * SQL function that computes keys by which rows can be partitioned and
 * aggregated.
 *
 * <p>Grouped window functions always occur in the GROUP BY clause. They often
 * have auxiliary functions that access information about the group. For
 * example, {@code HOP} is a group function, and its auxiliary functions are
 * {@code HOP_START} and {@code HOP_END}. Here they are used in a streaming
 * query:
 *
 * <blockquote><pre>
 * SELECT STREAM HOP_START(rowtime, INTERVAL '1' HOUR),
 *   HOP_END(rowtime, INTERVAL '1' HOUR),
 *   MIN(unitPrice)
 * FROM Orders
 * GROUP BY HOP(rowtime, INTERVAL '1' HOUR), productId
 * </pre></blockquote>
 */
public class SqlGroupFunction extends SqlFunction {
  /** The grouped function, if this an auxiliary function; null otherwise. */
  final SqlGroupFunction groupFunction;

  /** Creates a SqlGroupFunction.
   *
   * @param name Function name
   * @param kind Kind
   * @param groupFunction Group function, if this is an auxiliary;
   *                      null, if this is a group function
   * @param operandTypeChecker Operand type checker
   */
  public SqlGroupFunction(String name, SqlKind kind, SqlGroupFunction groupFunction,
      SqlOperandTypeChecker operandTypeChecker) {
    super(name, kind, ReturnTypes.ARG0, null,
        operandTypeChecker, SqlFunctionCategory.SYSTEM);
    this.groupFunction = groupFunction;
    if (groupFunction != null) {
      assert groupFunction.groupFunction == null;
    }
  }

  /** Creates a SqlGroupFunction.
   *
   * @param kind Kind; also determines function name
   * @param groupFunction Group function, if this is an auxiliary;
   *                      null, if this is a group function
   * @param operandTypeChecker Operand type checker
   */
  public SqlGroupFunction(SqlKind kind, SqlGroupFunction groupFunction,
      SqlOperandTypeChecker operandTypeChecker) {
    this(kind.name(), kind, groupFunction, operandTypeChecker);
  }

	/** Creates a SqlGroupFunction.
	 *
	 * @param name Function name
	 * @param kind Kind
	 * @param groupFunction Group function, if this is an auxiliary;
	 *                      null, if this is a group function
	 * @param returnTypeInference Inference of the functions return type
	 * @param operandTypeChecker Operand type checker
	 */
  public SqlGroupFunction(String name, SqlKind kind, SqlGroupFunction groupFunction,
      SqlReturnTypeInference returnTypeInference, SqlOperandTypeChecker operandTypeChecker) {
    super(name, kind, returnTypeInference, null, operandTypeChecker,
      SqlFunctionCategory.SYSTEM);
    this.groupFunction = groupFunction;
    if (groupFunction != null) {
      assert groupFunction.groupFunction == null;
    }
  }

  /** Creates an auxiliary function from this grouped window function.
   *
   * @param kind Kind; also determines function name
   */
  public SqlGroupFunction auxiliary(SqlKind kind) {
    return auxiliary(kind.name(), kind);
  }

  /** Creates an auxiliary function from this grouped window function.
   *
   * @param name Function name
   * @param kind Kind
   */
  public SqlGroupFunction auxiliary(String name, SqlKind kind) {
    return new SqlGroupFunction(name, kind, this, getOperandTypeChecker());
  }

  /** Returns a list of this grouped window function's auxiliary functions. */
  public List<SqlGroupFunction> getAuxiliaryFunctions() {
    return ImmutableList.of();
  }

  @Override public boolean isGroup() {
    // Auxiliary functions are not group functions
    return groupFunction == null;
  }

  @Override public boolean isGroupAuxiliary() {
    return groupFunction != null;
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    // Monotonic iff its first argument is, but not strict.
    //
    // Note: This strategy happens to works for all current group functions
    // (HOP, TUMBLE, SESSION). When there are exceptions to this rule, we'll
    // make the method abstract.
    return call.getOperandMonotonicity(0).unstrict();
  }
}

// End SqlGroupFunction.java
