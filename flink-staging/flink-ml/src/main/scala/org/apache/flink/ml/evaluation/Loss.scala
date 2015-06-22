package org.apache.flink.ml.evaluation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml._

import scala.reflect.ClassTag

/**
 * Loss function
 *
 * Takes a hole data set and then computes the score on them (obviously, again encoded in a DataSet)
 *
 * @tparam Y output type
 */
trait Loss[Y] {
  def loss(trueAndPredicted: DataSet[(Y, Y)]): DataSet[Double]
}

/**
 * Loss functions expressible as a mean of a function taking output pairs as input
 *
 * @param lossFct function to apply to all elements
 * @tparam Y output type
 */
class MeanLoss[Y: TypeInformation: ClassTag](lossFct: (Y, Y) => Double)
                                            (implicit yyt: TypeInformation[(Y, Y)])
extends Loss[Y] with Serializable {
  def loss(trueAndPredicted: DataSet[(Y, Y)]): DataSet[Double] =
    trueAndPredicted.map(yy => lossFct(yy._1, yy._2)).mean()
}


object Loss {
  /**
   * Squared loss function
   *
   * returns (y1 - y2)'
   *
   * @return a Loss object
   */
  def squaredLoss = new MeanLoss[Double]((y1,y2) => (y1 - y2) * (y1 - y2))

  /**
   * Zero One Loss Function
   *
   * returns 1 if outputs differ and 0 if they are equal
   *
   * @tparam T output type
   * @return a Loss object
   */
  def zeroOneLoss[T: TypeInformation: ClassTag] = new MeanLoss[T]((y1, y2) => if (y1 == y2) 0 else 1)

  /**
   * Zero One Loss Function also usable for score information
   *
   * returns 1 if sign of outputs differ and 0 if the signs are equal
   *
   * @return a Loss object
   */
  def zeroOneSignumLoss = new MeanLoss[Double]({ (y1, y2) =>
    val sy1 = scala.math.signum(y1)
    val sy2 = scala.math.signum(y2)
    if (sy1 == sy2) 0 else 1
  })

}
