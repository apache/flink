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

package org.apache.flink.api.table.validate

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.util.{ChainedSqlOperatorTable, ListSqlOperatorTable}
import org.apache.calcite.sql.{SqlFunction, SqlOperatorTable}
import org.apache.flink.api.table.ValidationException
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.functions.ScalarFunction
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * A catalog for looking up user-defined functions, used during validation phase.
  */
class FunctionCatalog {

  private val functionBuilders = mutable.HashMap.empty[String, Class[_]]
  private val sqlFunctions = mutable.ListBuffer[SqlFunction]()

  def registerFunction(name: String, builder: Class[_]): Unit =
    functionBuilders.put(name.toLowerCase, builder)

  def registerSqlFunction(sqlFunction: SqlFunction): Unit = {
    sqlFunctions --= sqlFunctions.filter(_.getName == sqlFunction.getName)
    sqlFunctions += sqlFunction
  }

  def getSqlOperatorTable: SqlOperatorTable =
    ChainedSqlOperatorTable.of(
      SqlStdOperatorTable.instance(),
      new ListSqlOperatorTable(sqlFunctions)
    )

  /**
    * Lookup and create an expression if we find a match.
    */
  def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val funcClass = functionBuilders
      .getOrElse(name.toLowerCase, throw ValidationException(s"Undefined function: $name"))
    withChildren(funcClass, children)
  }

  /**
    * Instantiate a function using the provided `children`.
    */
  private def withChildren(func: Class[_], children: Seq[Expression]): Expression = {
    func match {

      // user-defined scalar function call
      case sf if classOf[ScalarFunction].isAssignableFrom(sf) =>
        Try(UserDefinedFunctionUtils.instantiate(sf.asInstanceOf[Class[ScalarFunction]])) match {
          case Success(scalarFunction) => ScalarFunctionCall(scalarFunction, children)
          case Failure(e) => throw ValidationException(e.getMessage)
        }

      // general expression call
      case expression if classOf[Expression].isAssignableFrom(expression) =>
        // try to find a constructor accepts `Seq[Expression]`
        Try(func.getDeclaredConstructor(classOf[Seq[_]])) match {
          case Success(seqCtor) =>
            Try(seqCtor.newInstance(children).asInstanceOf[Expression]) match {
              case Success(expr) => expr
              case Failure(e) => throw new ValidationException(e.getMessage)
            }
          case Failure(e) =>
            val childrenClass = Seq.fill(children.length)(classOf[Expression])
            // try to find a constructor matching the exact number of children
            Try(func.getDeclaredConstructor(childrenClass: _*)) match {
              case Success(ctor) =>
                Try(ctor.newInstance(children: _*).asInstanceOf[Expression]) match {
                  case Success(expr) => expr
                  case Failure(exception) => throw ValidationException(exception.getMessage)
                }
              case Failure(exception) =>
                throw ValidationException(s"Invalid number of arguments for function $func")
            }
        }

      case _ =>
        throw ValidationException("Unsupported function.")
    }
  }

  /**
    * Drop a function and return if the function existed.
    */
  def dropFunction(name: String): Boolean =
    functionBuilders.remove(name.toLowerCase).isDefined

  /**
    * Drop all registered functions.
    */
  def clear(): Unit = functionBuilders.clear()
}

object FunctionCatalog {

  val buildInFunctions: Map[String, Class[_]] = Map(
    // logic
    "isNull" -> classOf[IsNull],
    "isNotNull" -> classOf[IsNotNull],
    "isTrue" -> classOf[IsTrue],
    "isFalse" -> classOf[IsFalse],

    // aggregate functions
    "avg" -> classOf[Avg],
    "count" -> classOf[Count],
    "max" -> classOf[Max],
    "min" -> classOf[Min],
    "sum" -> classOf[Sum],

    // string functions
    "charLength" -> classOf[CharLength],
    "initCap" -> classOf[InitCap],
    "like" -> classOf[Like],
    "lowerCase" -> classOf[Lower],
    "similar" -> classOf[Similar],
    "substring" -> classOf[Substring],
    "trim" -> classOf[Trim],
    "upperCase" -> classOf[Upper],
    "position" -> classOf[Position],
    "overlay" -> classOf[Overlay],

    // math functions
    "abs" -> classOf[Abs],
    "ceil" -> classOf[Ceil],
    "exp" -> classOf[Exp],
    "floor" -> classOf[Floor],
    "log10" -> classOf[Log10],
    "ln" -> classOf[Ln],
    "power" -> classOf[Power],
    "mod" -> classOf[Mod],
    "sqrt" -> classOf[Sqrt],

    // temporal functions
    "extract" -> classOf[Extract],
    "currentDate" -> classOf[CurrentDate],
    "currentTime" -> classOf[CurrentTime],
    "currentTimestamp" -> classOf[CurrentTimestamp],
    "localTime" -> classOf[LocalTime],
    "localTimestamp" -> classOf[LocalTimestamp],
    "quarter" -> classOf[Quarter]

    // TODO implement function overloading here
    // "floor" -> classOf[TemporalFloor]
    // "ceil" -> classOf[TemporalCeil]
  )

  /**
    * Create a new function catalog with build-in functions.
    */
  def withBuildIns: FunctionCatalog = {
    val catalog = new FunctionCatalog()
    buildInFunctions.foreach { case (n, c) => catalog.registerFunction(n, c) }
    catalog
  }
}
