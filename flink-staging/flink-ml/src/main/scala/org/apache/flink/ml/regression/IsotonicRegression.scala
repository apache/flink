package org.apache.flink.ml.regression

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.{FitOperation, Predictor}
import org.apache.flink.api.common.operators.Order

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
;

class IsotonicRegression extends Predictor[IsotonicRegression] {

  var fitted: Option[Array[(Double,Double,Double)]] = None

}

object IsotonicRegression {

  implicit val fitIR = new FitOperation[IsotonicRegression, (Double,Double,Double)] {

    override def fit(instance: IsotonicRegression,
                     fitParameters: ParameterMap,
                     input: DataSet[(Double, Double, Double)]): Unit = {

      // assume input is correctly range partitioned :-)

      val parallelStepResult = input
          .mapPartition(partition => {
            val buffer : ArrayBuffer[(Double,Double,Double)] = new ArrayBuffer[(Double,Double,Double)]
            buffer ++= partition
            buffer.sortBy(x => (x._2, x._1))
            Seq(buffer.toArray)
          })
          .flatMap(arr => poolAdjacentViolators(arr))
          .collect()
          .sortBy(x => (x._2, x._1))

      instance.fitted = Some(poolAdjacentViolators(parallelStepResult.toArray))

    }

    /**
     * Performs a pool adjacent violators algorithm (PAV).
     * Uses approach with single processing of data where violators
     * in previously processed data created by pooling are fixed immediately.
     * Uses optimization of discovering monotonicity violating sequences (blocks).
     *
     * @param input Input data of tuples (label, feature, weight).
     * @return Result tuples (label, feature, weight) where labels were updated
     *         to form a monotone sequence as per isotonic regression definition.
     */
    private def poolAdjacentViolators(input: Array[(Double, Double, Double)]): Array[(Double, Double, Double)] = {

      if (input.isEmpty) {
        return Array.empty
      }

      // Pools sub array within given bounds assigning weighted average value to all elements.
      def pool(input: Array[(Double, Double, Double)], start: Int, end: Int): Unit = {
        val poolSubArray = input.slice(start, end + 1)

        val weightedSum = poolSubArray.map(lp => lp._1 * lp._3).sum
        val weight = poolSubArray.map(_._3).sum

        var i = start
        while (i <= end) {
          input(i) = (weightedSum / weight, input(i)._2, input(i)._3)
          i = i + 1
        }
      }

      var i = 0
      val len = input.length
      while (i < len) {
        var j = i

        // Find monotonicity violating sequence, if any.
        while (j < len - 1 && input(j)._1 > input(j + 1)._1) {
          j = j + 1
        }

        // If monotonicity was not violated, move to next data point.
        if (i == j) {
          i = i + 1
        } else {
          // Otherwise pool the violating sequence
          // and check if pooling caused monotonicity violation in previously processed points.
          while (i >= 0 && input(i)._1 > input(i + 1)._1) {
            pool(input, i, j)
            i = i - 1
          }

          i = j
        }
      }
      // For points having the same prediction, we only keep two boundary points.
      val compressed = ArrayBuffer.empty[(Double, Double, Double)]

      var (curLabel, curFeature, curWeight) = input.head
      var rightBound = curFeature
      def merge(): Unit = {
        compressed += ((curLabel, curFeature, curWeight))
        if (rightBound > curFeature) {
          compressed += ((curLabel, rightBound, 0.0))
        }
      }
      i = 1
      while (i < input.length) {
        val (label, feature, weight) = input(i)
        if (label == curLabel) {
          curWeight += weight
          rightBound = feature
        } else {
          merge()
          curLabel = label
          curFeature = feature
          curWeight = weight
          rightBound = curFeature
        }
        i += 1
      }
      merge()

      compressed.toArray
    }

  }

}
