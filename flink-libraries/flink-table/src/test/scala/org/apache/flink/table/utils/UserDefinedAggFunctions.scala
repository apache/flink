package org.apache.flink.table.utils

import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.lang.{Integer => JInt}
import java.lang.{Float => JFloat}
import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, TupleTypeInfo}
import org.apache.flink.table.api.Types

/**
  * User-defined aggregation function to compute the TOP 10 most visited pages.
  * We use and Array[Tuple2[Int, Float]] as accumulator to store the 10 most visited pages.
  *
  * The result is emitted as Array as well.
  */
class Top10 extends AggregateFunction[Array[JTuple2[JInt, JFloat]], Array[JTuple2[JInt, JFloat]]] {

  @Override
  def createAccumulator(): Array[JTuple2[JInt, JFloat]] = {
    new Array[JTuple2[JInt, JFloat]](10)
  }

  /**
    * Adds a new pages and count to the Top10 pages if necessary.
    *
    * @param acc The current top 10
    * @param adId The id of the ad
    * @param revenue The revenue for the ad
    */
  def accumulate(acc: Array[JTuple2[JInt, JFloat]], adId: Int, revenue: Float) {

    var i = 9
    var skipped = 0

    // skip positions without records
    while (i >= 0 && acc(i) == null) {
      if (acc(i) == null) {
        // continue until first entry in the top10 list
        i -= 1
      }
    }
    // backward linear search for insert position
    while (i >= 0 && revenue > acc(i).f1) {
      // check next entry
      skipped += 1
      i -= 1
    }

    // set if necessary
    if (i < 9) {
      // move entries with lower count by one position
      if (i < 8 && skipped > 0) {
        System.arraycopy(acc, i + 1, acc, i + 2, skipped)
      }

      // add page to top10 list
      acc(i + 1) = JTuple2.of(adId, revenue)
    }
  }

  override def getValue(acc: Array[JTuple2[JInt, JFloat]]): Array[JTuple2[JInt, JFloat]] = acc

  def resetAccumulator(acc: Array[JTuple2[JInt, JFloat]]): Unit = {
    util.Arrays.fill(acc.asInstanceOf[Array[Object]], null)
  }

  def merge(
      acc: Array[JTuple2[JInt, JFloat]],
      its: java.lang.Iterable[Array[JTuple2[JInt, JFloat]]]): Unit = {

    val it = its.iterator()
    while(it.hasNext) {
      val acc2 = it.next()

      var i = 0
      var i2 = 0
      while (i < 10 && i2 < 10 && acc2(i2) != null) {
        if (acc(i) == null) {
          // copy to empty place
          acc(i) = acc2(i2)
          i += 1
          i2 += 1
        } else if (acc(i).f1.asInstanceOf[Float] >= acc2(i2).f1.asInstanceOf[Float]) {
          // forward to next
          i += 1
        } else {
          // shift and copy
          System.arraycopy(acc, i, acc, i + 1, 9 - i)
          acc(i) = acc2(i2)
          i += 1
          i2 += 1
        }
      }
    }
  }

  override def getAccumulatorType: TypeInformation[Array[JTuple2[JInt, JFloat]]] = {
    ObjectArrayTypeInfo.getInfoFor(new TupleTypeInfo[JTuple2[JInt, JFloat]](Types.INT, Types.FLOAT))
  }

  override def getResultType: TypeInformation[Array[JTuple2[JInt, JFloat]]] = {
    ObjectArrayTypeInfo.getInfoFor(new TupleTypeInfo[JTuple2[JInt, JFloat]](Types.INT, Types.FLOAT))
  }
}
