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

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.validate.FunctionCatalog.FunctionBuilder

/**
  * A catalog for looking up user defined functions, used by an Analyzer.
  *
  * Note: this is adapted from Spark's FunctionRegistry.
  */
trait FunctionCatalog {

  def registerFunction(name: String, builder: FunctionBuilder): Unit

  /**
    * Lookup and create an expression if we find a match.
    */
  def lookupFunction(name: String, children: Seq[Expression]): Expression

  /**
    * Drop a function and return if the function existed.
    */
  def dropFunction(name: String): Boolean

  /**
    * Drop all registered functions.
    */
  def clear(): Unit
}

class SimpleFunctionCatalog extends FunctionCatalog {
  private val functionBuilders = new CaseInsensitiveStringKeyHashMap[FunctionBuilder]

  override def registerFunction(name: String, builder: FunctionBuilder): Unit =
    functionBuilders.put(name, builder)

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val func = functionBuilders.get(name).getOrElse {
      throw new ValidationException(s"undefined function $name")
    }
    func(children)
  }

  override def dropFunction(name: String): Boolean =
    functionBuilders.remove(name).isDefined

  override def clear(): Unit = functionBuilders.clear()
}

object FunctionCatalog {
  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, FunctionBuilder] = Map(
    // aggregate functions
    expression[Avg]("avg"),
    expression[Count]("count"),
    expression[Max]("max"),
    expression[Min]("min"),
    expression[Sum]("sum"),

    // string functions
    expression[CharLength]("charLength"),
    expression[InitCap]("initCap"),
    expression[Like]("like"),
    expression[Lower]("lowerCase"),
    expression[Similar]("similar"),
    expression[SubString]("subString"),
    expression[Trim]("trim"),
    expression[Upper]("upperCase"),

    // math functions
    expression[Abs]("abs"),
    expression[Ceil]("ceil"),
    expression[Exp]("exp"),
    expression[Floor]("floor"),
    expression[Log10]("log10"),
    expression[Ln]("ln"),
    expression[Power]("power"),
    expression[Mod]("mod")
  )

  val builtin: SimpleFunctionCatalog = {
    val sfc = new SimpleFunctionCatalog
    expressions.foreach { case (name, builder) => sfc.registerFunction(name, builder) }
    sfc
  }

  def expression[T <: Expression](name: String)
    (implicit tag: ClassTag[T]): (String, FunctionBuilder) = {

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = Try(tag.runtimeClass.getDeclaredConstructor(classOf[Seq[_]])).toOption
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) => throw new ValidationException(e.getMessage)
        }
      } else {
        // Otherwise, find an ctor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = Try(tag.runtimeClass.getDeclaredConstructor(params : _*)) match {
          case Success(e) =>
            e
          case Failure(e) =>
            throw new ValidationException(s"Invalid number of arguments for function $name")
        }
        Try(f.newInstance(expressions : _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new ValidationException(e.getCause.getMessage)
        }
      }
    }
    (name, builder)
  }
}

class CaseInsensitiveStringKeyHashMap[T] {
  private val base = new collection.mutable.HashMap[String, T]()

  private def normalizer: String => String = _.toLowerCase

  def apply(key: String): T = base(normalizer(key))

  def get(key: String): Option[T] = base.get(normalizer(key))

  def put(key: String, value: T): Option[T] = base.put(normalizer(key), value)

  def remove(key: String): Option[T] = base.remove(normalizer(key))

  def iterator: Iterator[(String, T)] = base.toIterator

  def clear(): Unit = base.clear()
}
