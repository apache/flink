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

package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

case class Md5(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = STRING_TYPE_INFO :: Nil

  override def toString: String = s"($child).md5()"
}

case class Sha1(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = STRING_TYPE_INFO :: Nil

  override def toString: String = s"($child).sha1()"
}

case class Sha224(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = STRING_TYPE_INFO :: Nil

  override def toString: String = s"($child).sha224()"
}

case class Sha256(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = STRING_TYPE_INFO :: Nil

  override def toString: String = s"($child).sha256()"
}

case class Sha384(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = STRING_TYPE_INFO :: Nil

  override def toString: String = s"($child).sha384()"
}

case class Sha512(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = STRING_TYPE_INFO :: Nil

  override def toString: String = s"($child).sha512()"
}

case class Sha2(child: PlannerExpression, hashLength: PlannerExpression)
    extends BinaryExpression with InputTypeSpec {

  override private[flink] def left = child
  override private[flink] def right = hashLength

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    STRING_TYPE_INFO :: INT_TYPE_INFO :: Nil

  override def toString: String = s"($child).sha2($hashLength)"

}


