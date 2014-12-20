package org.apache.flink.api.scala.streaming

import org.apache.flink.streaming.util.keys.{ FieldsKeySelector => JavaSelector }
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple

class FieldsKeySelector[IN](fields: Int*) extends KeySelector[IN, Tuple] {

  val t: Tuple = JavaSelector.tupleClasses(fields.length - 1).newInstance()

  override def getKey(value: IN): Tuple =

    value match {
      case prod: Product => {
        for (i <- 0 to fields.length - 1) {
          t.setField(prod.productElement(fields(i)), i)
        }
        t
      }
      case tuple: Tuple => {
        for (i <- 0 to fields.length - 1) {
          t.setField(tuple.getField(fields(i)), i)
        }
        t
      }
      case _ => throw new RuntimeException("Only tuple types are supported")
    }

}