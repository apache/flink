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

package eu.stratosphere.scala

import java.lang.annotation.Annotation
import eu.stratosphere.api.operators.Contract
import eu.stratosphere.scala.analysis.UDF
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.analysis.UDF2
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.pact.compiler.plan.OptimizerNode
import eu.stratosphere.api.operators.AbstractPact
import eu.stratosphere.scala.analysis.UDF0
import eu.stratosphere.api.functions.StubAnnotation

trait ScalaContract[T] { this: Contract =>
  def getUDF(): UDF[T]
  def getKeys: Seq[FieldSelector] = Seq()
  def persistConfiguration(): Unit = {}
  
  var persistHints: () => Unit = { () => }
  
  def persistConfiguration(optimizerNode: Option[OptimizerNode]): Unit = {

    ScalaContract.this match {
      
      case contract: AbstractPact[_] => {
        for ((key, inputNum) <- getKeys.zipWithIndex) {
          
          val source = key.selectedFields.toSerializerIndexArray
          val target = optimizerNode map { _.getRemappedKeys(inputNum) } getOrElse { contract.getKeyColumns(inputNum) }

          assert(source.length == target.length, "Attempt to write " + source.length + " key indexes to an array of size " + target.length)
          System.arraycopy(source, 0, target, 0, source.length)
        }
      }
      
      case _ if getKeys.size > 0 => throw new UnsupportedOperationException("Attempted to set keys on a contract that doesn't support them")
      
      case _ =>
    }

    persistHints()
    persistConfiguration()
  }
  
  protected def annotations: Seq[Annotation] = Seq()

  override def getUserCodeAnnotation[A <: Annotation](annotationClass: Class[A]): A = {
    val res = annotations find { _.annotationType().equals(annotationClass) } map { _.asInstanceOf[A] } getOrElse null.asInstanceOf[A]
//    println("returning ANOOT: " + res + " FOR: " + annotationClass.toString)
//    res match {
//      case r : StubAnnotation.ConstantFieldsFirst => println("CONSTANT FIELDS FIRST: " + r.value().mkString(","))
//      case r : StubAnnotation.ConstantFieldsSecond => println("CONSTANT FIELDS SECOND: " + r.value().mkString(","))
//      case _ =>
//    }
    res
  }
}

trait NoOpScalaContract[In, Out] extends ScalaContract[Out] { this: Contract =>
}

trait UnionScalaContract[In] extends NoOpScalaContract[In, In] { this: Contract =>
  override def getUDF(): UDF1[In, In]
}

trait HigherOrderScalaContract[T] extends ScalaContract[T] { this: Contract =>
  override def getUDF(): UDF0[T]
}

trait BulkIterationScalaContract[T] extends HigherOrderScalaContract[T] { this: Contract =>
}

trait WorksetIterationScalaContract[T] extends HigherOrderScalaContract[T] { this: Contract =>
  val key: FieldSelector
}

trait OneInputScalaContract[In, Out] extends ScalaContract[Out] { this: Contract =>
  override def getUDF(): UDF1[In, Out]
}

trait TwoInputScalaContract[In1, In2, Out] extends ScalaContract[Out] { this: Contract =>
  override def getUDF(): UDF2[In1, In2, Out]
}

trait OneInputKeyedScalaContract[In, Out] extends OneInputScalaContract[In, Out] { this: Contract =>
  val key: FieldSelector
  override def getKeys = Seq(key)
}

trait TwoInputKeyedScalaContract[LeftIn, RightIn, Out] extends TwoInputScalaContract[LeftIn, RightIn, Out] { this: Contract =>
  val leftKey: FieldSelector
  val rightKey: FieldSelector
  override def getKeys = Seq(leftKey, rightKey)
}