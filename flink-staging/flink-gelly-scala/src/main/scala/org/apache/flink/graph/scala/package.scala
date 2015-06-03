package org.apache.flink.graph


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.graph.{Graph => JGraph}

import _root_.scala.reflect.ClassTag


package object scala {
    private[flink] def wrapGraph[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV: TypeInformation : ClassTag](javagraph: JGraph[K, VV, EV]) = new Graph[K, VV, EV](javagraph)
}
