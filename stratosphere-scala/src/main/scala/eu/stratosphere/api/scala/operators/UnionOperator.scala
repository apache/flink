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

package eu.stratosphere.api.scala.operators

import language.experimental.macros
import eu.stratosphere.api.scala.UnionScalaOperator
import eu.stratosphere.api.scala.DataSet
import eu.stratosphere.api.scala.analysis.UDF2
import eu.stratosphere.api.common.operators.Union
import eu.stratosphere.types.Record

object UnionOperator {

  def impl[In](firstInput: DataSet[In], secondInput: DataSet[In]): DataSet[In] = {
    val union = new Union[Record](firstInput.contract, secondInput.contract) with UnionScalaOperator[In] {
      private val inputUDT = firstInput.contract.getUDF().outputUDT
      private val udf: UDF2[In, In, In] = new UDF2(inputUDT, inputUDT, inputUDT)

      override def getUDF = udf;
    }
    return new DataSet(union)
  }
}
