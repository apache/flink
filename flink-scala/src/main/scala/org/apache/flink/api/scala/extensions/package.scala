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
package org.apache.flink.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.extensions.acceptPartialFunctions._

import scala.reflect.ClassTag

package object extensions {

  implicit def acceptPartialFunctionsOnDataSet[T: TypeInformation](ds: DataSet[T]): OnDataSet[T] =
    new OnDataSet[T](ds)

  implicit def acceptPartialFunctionsOnJoinFunctionAssigner[L: TypeInformation, R: TypeInformation](
      ds: JoinFunctionAssigner[L, R]): OnJoinFunctionAssigner[L, R] =
    new OnJoinFunctionAssigner[L, R](ds)

  implicit def acceptPartialFunctionsOnCrossDataSet[L: TypeInformation, R: TypeInformation](
      ds: CrossDataSet[L, R]): OnCrossDataSet[L, R] =
    new OnCrossDataSet[L, R](ds)

  implicit def acceptPartialFunctionsOnGroupedDataSet[T: TypeInformation: ClassTag](
      ds: GroupedDataSet[T]):
      OnGroupedDataSet[T] =
    new OnGroupedDataSet[T](ds)

  implicit def acceptPartialFunctionsOnCoGroupDataSet[L: TypeInformation, R: TypeInformation](
      ds: CoGroupDataSet[L, R]): OnCoGroupDataSet[L, R] =
    new OnCoGroupDataSet[L, R](ds)

  implicit def acceptPartialFunctionsOnHalfUnfinishedKeyPairOperation[L: TypeInformation, R: TypeInformation, O: TypeInformation](
      ds: HalfUnfinishedKeyPairOperation[L, R, O]): OnHalfUnfinishedKeyPairOperation[L, R, O] =
    new OnHalfUnfinishedKeyPairOperation[L, R, O](ds)

  implicit def acceptPartialFunctionsOnUnfinishedKeyPairOperation[L: TypeInformation, R: TypeInformation, O: TypeInformation](
      ds: UnfinishedKeyPairOperation[L, R, O]): OnUnfinishedKeyPairOperation[L, R, O] =
    new OnUnfinishedKeyPairOperation[L, R, O](ds)

}
