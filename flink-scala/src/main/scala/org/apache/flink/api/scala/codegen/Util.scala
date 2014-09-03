/**
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


package org.apache.flink.api.scala.codegen

import language.experimental.macros
import scala.reflect.macros.Context

import org.apache.flink.api.scala.analysis.UDT

object Util {
  
  implicit def createUDT[T]: UDT[T] = macro createUDTImpl[T]

  def createUDTImpl[T: c.WeakTypeTag](c: Context): c.Expr[UDT[T]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)

    val (udt, createUdt) = slave.mkUdtClass[T]

    val udtResult = reify {
      c.Expr[UDT[T]](createUdt).splice
    }
    
    c.Expr[UDT[T]](Block(List(udt), udtResult.tree))
  }

  // filter out forwards that dont forward from one field position to the same field position
  def filterNonForwards(from: Array[Int], to: Array[Int]): Array[Int] = {
    from.zip(to).filter( z => z._1 == z._2).map { _._1}
  }
}
