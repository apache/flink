/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala.codegen

import language.experimental.macros
import scala.reflect.macros.Context
import eu.stratosphere.scala.analysis.UDT

object UDTUtil {
  
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
}
