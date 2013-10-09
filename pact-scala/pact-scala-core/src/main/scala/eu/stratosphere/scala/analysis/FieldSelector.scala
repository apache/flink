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

package eu.stratosphere.scala.analysis

import scala.language.experimental.macros
import scala.language.implicitConversions

/**
 * Instances of this class are typically created by the field selector macros fieldSelectorImpl
 * and keySelectorImpl in {@link FieldSelectorMacros}. 
 *
 * In addition to the language restrictions applied to the lambda expression, the selected fields
 * must also be top-level. Nested fields (such as a list element or an inner instance of a
 * recursive type) are not allowed.
 *
 * @param selection The selected fields
 */
class FieldSelector(udt: UDT[_], selection: List[Int]) extends Serializable {
  
  val inputFields = FieldSet.newInputSet(udt.numFields)
  val selectionIndices = udt.getSelectionIndices(selection)
  val selectedFields = inputFields.select(selectionIndices)

  for (field <- inputFields.diff(selectedFields))
    field.isUsed = false
  
  def copy() = new FieldSelector(udt, selection)
}