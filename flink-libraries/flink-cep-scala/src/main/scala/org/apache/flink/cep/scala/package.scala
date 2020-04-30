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
package org.apache.flink.cep

import java.util.{List => JList, Map => JMap}

import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.cep.{PatternStream => JPatternStream}

package object scala {

  import collection.JavaConverters._
  import collection.Map

  /**
    * Utility method to wrap [[org.apache.flink.cep.PatternStream]] for usage with the Scala API.
    *
    * @param javaPatternStream The underlying pattern stream from the Java API
    * @tparam T Type of the events
    * @return A pattern stream from the Scala API which wraps a pattern stream from the Java API
    */
  private[flink] def wrapPatternStream[T](javaPatternStream: JPatternStream[T])
  : scala.PatternStream[T] = {
    Option(javaPatternStream) match {
      case Some(p) => PatternStream[T](p)
      case None =>
        throw new IllegalArgumentException("PatternStream from Java API must not be null.")
    }
  }

  private[flink] def cleanClosure[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  private[flink] def mapToScala[T](map: JMap[String, JList[T]]): Map[String, Iterable[T]] = {
    map.asScala.mapValues(_.asScala.toIterable)
  }
}

