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
package org.apache.flink.cep.scala

import org.apache.flink.cep.pattern.{Pattern => JPattern}
import org.apache.flink.cep.pattern.conditions.IterativeCondition.{Context => JContext}
import org.apache.flink.cep.scala.conditions.Context
import scala.collection.JavaConverters._

package object pattern {
  /**
    * Utility method to wrap [[org.apache.flink.cep.pattern.Pattern]] and its subclasses
    * for usage with the Scala API.
    *
    * @param javaPattern The underlying pattern from the Java API
    * @tparam T Base type of the elements appearing in the pattern
    * @tparam F Subtype of T to which the current pattern operator is constrained
    * @return A pattern from the Scala API which wraps the pattern from the Java API
    */
  private[flink] def wrapPattern[T, F <: T](javaPattern: JPattern[T, F])
  : Option[Pattern[T, F]] = javaPattern match {
    case p: JPattern[T, F] => Some(Pattern[T, F](p))
    case _ => None
  }

  private[pattern] class JContextWrapper[F](private val jContext: JContext[F])
    extends Context[F] with Serializable {

    override def getEventsForPattern(name: String): Iterable[F] =
      jContext.getEventsForPattern(name).asScala
  }
}

