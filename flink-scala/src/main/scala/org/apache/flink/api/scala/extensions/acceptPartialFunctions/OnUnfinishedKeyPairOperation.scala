package org.apache.flink.api.scala.extensions.acceptPartialFunctions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{HalfUnfinishedKeyPairOperation, UnfinishedKeyPairOperation}

class OnUnfinishedKeyPairOperation[L: TypeInformation, R: TypeInformation, O: TypeInformation](ds: UnfinishedKeyPairOperation[L, R, O]) {

  def whereClause[K: TypeInformation](fun: (L) => K): HalfUnfinishedKeyPairOperation[L, R, O] =
    ds.where(fun)

}
