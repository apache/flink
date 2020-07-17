package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeutils.TypeInformationTestBase
import org.apache.flink.api.scala._
/**
 * Test for [[SealedTraitTypeInfo]].
 */
class SealedTraitTypeInfoTest extends TypeInformationTestBase[SealedTraitTypeInfo[ADT]] {
  override protected def getTestData: Array[SealedTraitTypeInfo[ADT]] = Array(
    new SealedTraitTypeInfo[ADT](classOf[ADT], Array(
      classOf[AdtOne], classOf[AdtTwo]
    ),Array(
      createTypeInformation[AdtOne], createTypeInformation[AdtTwo]
    ))
  )
}
