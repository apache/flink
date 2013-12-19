/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.scala

import java.lang.annotation.Annotation
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.scala.analysis.UDF
import eu.stratosphere.api.scala.analysis.UDF1
import eu.stratosphere.api.scala.analysis.UDF2
import eu.stratosphere.api.scala.analysis.FieldSelector
import eu.stratosphere.compiler.dag.OptimizerNode
import eu.stratosphere.api.common.operators.AbstractUdfOperator
import eu.stratosphere.api.scala.analysis.UDF0
import eu.stratosphere.api.record.functions.FunctionAnnotation

trait ScalaOperator[T] { this: Operator =>
  def getUDF(): UDF[T]
  def getKeys: Seq[FieldSelector] = Seq()
  def persistConfiguration(): Unit = {}
  
  var persistHints: () => Unit = { () => }
  
  def persistConfiguration(optimizerNode: Option[OptimizerNode]): Unit = {

    ScalaOperator.this match {
      
      case contract: AbstractUdfOperator[_] => {
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
//      case r : FunctionAnnotation.ConstantFieldsFirst => println("CONSTANT FIELDS FIRST: " + r.value().mkString(","))
//      case r : FunctionAnnotation.ConstantFieldsSecond => println("CONSTANT FIELDS SECOND: " + r.value().mkString(","))
//      case _ =>
//    }
    res
  }
}

trait NoOpScalaOperator[In, Out] extends ScalaOperator[Out] { this: Operator =>
}

trait UnionScalaOperator[In] extends NoOpScalaOperator[In, In] { this: Operator =>
  override def getUDF(): UDF1[In, In]
}

trait HigherOrderScalaOperator[T] extends ScalaOperator[T] { this: Operator =>
  override def getUDF(): UDF0[T]
}

trait BulkIterationScalaOperator[T] extends HigherOrderScalaOperator[T] { this: Operator =>
}

trait DeltaIterationScalaOperator[T] extends HigherOrderScalaOperator[T] { this: Operator =>
  val key: FieldSelector
}

trait OneInputScalaOperator[In, Out] extends ScalaOperator[Out] { this: Operator =>
  override def getUDF(): UDF1[In, Out]
}

trait TwoInputScalaOperator[In1, In2, Out] extends ScalaOperator[Out] { this: Operator =>
  override def getUDF(): UDF2[In1, In2, Out]
}

trait OneInputKeyedScalaOperator[In, Out] extends OneInputScalaOperator[In, Out] { this: Operator =>
  val key: FieldSelector
  override def getKeys = Seq(key)
}

trait TwoInputKeyedScalaOperator[LeftIn, RightIn, Out] extends TwoInputScalaOperator[LeftIn, RightIn, Out] { this: Operator =>
  val leftKey: FieldSelector
  val rightKey: FieldSelector
  override def getKeys = Seq(leftKey, rightKey)
}