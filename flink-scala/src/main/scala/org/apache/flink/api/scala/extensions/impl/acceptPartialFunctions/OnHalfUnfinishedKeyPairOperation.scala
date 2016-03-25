package org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.HalfUnfinishedKeyPairOperation

class OnHalfUnfinishedKeyPairOperation[L: TypeInformation, R: TypeInformation, O: TypeInformation](ds: HalfUnfinishedKeyPairOperation[L, R, O]) {

  def isEqualTo[K: TypeInformation](fun: R => K): O =
    ds.equalTo(fun)

}
