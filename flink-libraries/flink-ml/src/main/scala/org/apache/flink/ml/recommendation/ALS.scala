/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.recommendation

import java.{util, lang}

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.memory.{DataOutputView, DataInputView}
import org.apache.flink.ml.common._
import org.apache.flink.ml.pipeline.{FitOperation, PredictDataSetOperation, Predictor}
import org.apache.flink.types.Value
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.{Partitioner => FlinkPartitioner, GroupReduceFunction, CoGroupFunction}

import com.github.fommil.netlib.BLAS.{ getInstance => blas }
import com.github.fommil.netlib.LAPACK.{ getInstance => lapack }
import org.netlib.util.intW

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** Alternating least squares algorithm to calculate a matrix factorization.
  *
  * Given a matrix `R`, ALS calculates two matrices `U` and `V` such that `R ~~ U^TV`. The
  * unknown row dimension is given by the number of latent factors. Since matrix factorization
  * is often used in the context of recommendation, we'll call the first matrix the user and the
  * second matrix the item matrix. The `i`th column of the user matrix is `u_i` and the `i`th
  * column of the item matrix is `v_i`. The matrix `R` is called the ratings matrix and
  * `(R)_{i,j} = r_{i,j}`.
  *
  * In order to find the user and item matrix, the following problem is solved:
  *
  * `argmin_{U,V} sum_(i,j\ with\ r_{i,j} != 0) (r_{i,j} - u_{i}^Tv_{j})^2 +
  * lambda (sum_(i) n_{u_i} ||u_i||^2 + sum_(j) n_{v_j} ||v_j||^2)`
  *
  * with `\lambda` being the regularization factor, `n_{u_i}` being the number of items the user `i`
  * has rated and `n_{v_j}` being the number of times the item `j` has been rated. This
  * regularization scheme to avoid overfitting is called weighted-lambda-regularization. Details
  * can be found in the work of [[http://dx.doi.org/10.1007/978-3-540-68880-8_32 Zhou et al.]].
  *
  * By fixing one of the matrices `U` or `V` one obtains a quadratic form which can be solved. The
  * solution of the modified problem is guaranteed to decrease the overall cost function. By
  * applying this step alternately to the matrices `U` and `V`, we can iteratively improve the
  * matrix factorization.
  *
  * The matrix `R` is given in its sparse representation as a tuple of `(i, j, r)` where `i` is the
  * row index, `j` is the column index and `r` is the matrix value at position `(i,j)`.
  *
  * @example
  *          {{{
  *             val inputDS: DataSet[(Int, Int, Double)] = env.readCsvFile[(Int, Int, Double)](
  *               pathToTrainingFile)
  *
  *             val als = ALS()
  *               .setIterations(10)
  *               .setNumFactors(10)
  *
  *             als.fit(inputDS)
  *
  *             val data2Predict: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)](pathToData)
  *
  *             als.predict(data2Predict)
  *          }}}
  *
  * =Parameters=
  *
  *  - [[org.apache.flink.ml.recommendation.ALS.NumFactors]]:
  *  The number of latent factors. It is the dimension of the calculated user and item vectors.
  *  (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.recommendation.ALS.Lambda]]:
  *  Regularization factor. Tune this value in order to avoid overfitting/generalization.
  *  (Default value: '''1''')
  *
  *  - [[org.apache.flink.ml.regression.MultipleLinearRegression.Iterations]]:
  *  The number of iterations to perform. (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.recommendation.ALS.Blocks]]:
  *  The number of blocks into which the user and item matrix a grouped. The fewer
  *  blocks one uses, the less data is sent redundantly. However, bigger blocks entail bigger
  *  update messages which have to be stored on the Heap. If the algorithm fails because of
  *  an OutOfMemoryException, then try to increase the number of blocks. (Default value: '''None''')
  *
  *  - [[org.apache.flink.ml.recommendation.ALS.Seed]]:
  *  Random seed used to generate the initial item matrix for the algorithm.
  *  (Default value: '''0''')
  *
  *  - [[org.apache.flink.ml.recommendation.ALS.TemporaryPath]]:
  *  Path to a temporary directory into which intermediate results are stored. If
  *  this value is set, then the algorithm is split into two preprocessing steps, the ALS iteration
  *  and a post-processing step which calculates a last ALS half-step. The preprocessing steps
  *  calculate the [[org.apache.flink.ml.recommendation.ALS.OutBlockInformation]] and
  *  [[org.apache.flink.ml.recommendation.ALS.InBlockInformation]] for the given rating matrix.
  *  The result of the individual steps are stored in the specified directory. By splitting the
  *  algorithm into multiple smaller steps, Flink does not have to split the available memory
  *  amongst too many operators. This allows the system to process bigger individual messasges and
  *  improves the overall performance. (Default value: '''None''')
  *
  * The ALS implementation is based on Spark's MLLib implementation of ALS which you can find
  * [[https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/
  * recommendation/ALS.scala here]].
  */
class ALS extends Predictor[ALS] {

  import ALS._

  // Stores the matrix factorization after the fitting phase
  var factorsOption: Option[(DataSet[Factors], DataSet[Factors])] = None

  /** Sets the number of latent factors/row dimension of the latent model
    *
    * @param numFactors
    * @return
    */
  def setNumFactors(numFactors: Int): ALS = {
    parameters.add(NumFactors, numFactors)
    this
  }

  /** Sets the regularization coefficient lambda
    *
    * @param lambda
    * @return
    */
  def setLambda(lambda: Double): ALS = {
    parameters.add(Lambda, lambda)
    this
  }

  /** Sets the number of iterations of the ALS algorithm
    * 
    * @param iterations
    * @return
    */
  def setIterations(iterations: Int): ALS = {
    parameters.add(Iterations, iterations)
    this
  }

  /** Sets the number of blocks into which the user and item matrix shall be partitioned
    * 
    * @param blocks
    * @return
    */
  def setBlocks(blocks: Int): ALS = {
    parameters.add(Blocks, blocks)
    this
  }

  /** Sets the random seed for the initial item matrix initialization
    * 
    * @param seed
    * @return
    */
  def setSeed(seed: Long): ALS = {
    parameters.add(Seed, seed)
    this
  }

  /** Sets the temporary path into which intermediate results are written in order to increase
    * performance.
    * 
    * @param temporaryPath
    * @return
    */
  def setTemporaryPath(temporaryPath: String): ALS = {
    parameters.add(TemporaryPath, temporaryPath)
    this
  }

  /** Empirical risk of the trained model (matrix factorization).
    *
    * @param labeledData Reference data
    * @param riskParameters Additional parameters for the empirical risk calculation
    * @return
    */
  def empiricalRisk(
      labeledData: DataSet[(Long, Long, Double)],
      riskParameters: ParameterMap = ParameterMap.Empty)
    : DataSet[Double] = {
    val resultingParameters = parameters ++ riskParameters

    val lambda = resultingParameters(Lambda)

    val data = labeledData map {
      x => (x._1, x._2)
    }

    factorsOption match {
      case Some((userFactors, itemFactors)) => {
        val predictions = data.join(userFactors, JoinHint.REPARTITION_HASH_SECOND).where(0)
          .equalTo(0).join(itemFactors, JoinHint.REPARTITION_HASH_SECOND).where("_1._2")
          .equalTo(0).map {
          triple => {
            val (((uID, iID), uFactors), iFactors) = triple

            val uFactorsVector = uFactors.factors
            val iFactorsVector = iFactors.factors

            val squaredUNorm2 = blas.ddot(
              uFactorsVector.length,
              uFactorsVector,
              1,
              uFactorsVector,
              1)
            val squaredINorm2 = blas.ddot(
              iFactorsVector.length,
              iFactorsVector,
              1,
              iFactorsVector,
              1)

            val prediction = blas.ddot(uFactorsVector.length, uFactorsVector, 1, iFactorsVector, 1)

            (uID, iID, prediction, squaredUNorm2, squaredINorm2)
          }
        }

        labeledData.join(predictions).where(0,1).equalTo(0,1) {
          (left, right) => {
            val (_, _, expected) = left
            val (_, _, predicted, squaredUNorm2, squaredINorm2) = right

            val residual = expected - predicted

            residual * residual + lambda * (squaredUNorm2 + squaredINorm2)
          }
        } reduce {
          _ + _
        }
      }

      case None => throw new RuntimeException("The ALS model has not been fitted to data. " +
        "Prior to predicting values, it has to be trained on data.")
    }
  }
}

object ALS {
  val USER_FACTORS_FILE = "userFactorsFile"
  val ITEM_FACTORS_FILE = "itemFactorsFile"

  // ========================================= Parameters ==========================================

  case object NumFactors extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(10)
  }

  case object Lambda extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(1.0)
  }

  case object Iterations extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(10)
  }

  case object Blocks extends Parameter[Int] {
    val defaultValue: Option[Int] = None
  }

  case object Seed extends Parameter[Long] {
    val defaultValue: Option[Long] = Some(0L)
  }

  case object TemporaryPath extends Parameter[String] {
    val defaultValue: Option[String] = None
  }

  // ==================================== ALS type definitions =====================================

  /** Representation of a user-item rating
    *
    * @param user User ID of the rating user
    * @param item Item iD of the rated item
    * @param rating Rating value
    */
  case class Rating(user: Long, item: Long, rating: Double)

  /** Latent factor model vector
    *
    * @param id
    * @param factors
    */
  case class Factors(id: Long, factors: Array[Double]) {
    override def toString = s"($id, ${factors.mkString(",")})"
  }

  case class Factorization(userFactors: DataSet[Factors], itemFactors: DataSet[Factors])

  case class OutBlockInformation(elementIDs: Array[Long], outLinks: OutLinks) {
    override def toString: String = {
      s"OutBlockInformation:((${elementIDs.mkString(",")}), ($outLinks))"
    }
  }

  class OutLinks(var links: Array[scala.collection.mutable.BitSet]) extends Value {
    def this() = this(null)

    override def toString: String = {
      s"${links.mkString("\n")}"
    }

    override def write(out: DataOutputView): Unit = {
      out.writeInt(links.length)
      links foreach {
        link => {
          val bitMask = link.toBitMask
          out.writeInt(bitMask.length)
          for (element <- bitMask) {
            out.writeLong(element)
          }
        }
      }
    }

    override def read(in: DataInputView): Unit = {
      val length = in.readInt()
      links = new Array[scala.collection.mutable.BitSet](length)

      for (i <- 0 until length) {
        val bitMaskLength = in.readInt()
        val bitMask = new Array[Long](bitMaskLength)
        for (j <- 0 until bitMaskLength) {
          bitMask(j) = in.readLong()
        }
        links(i) = mutable.BitSet.fromBitMask(bitMask)
      }
    }

    def apply(idx: Int) = links(idx)
  }

  case class InBlockInformation(elementIDs: Array[Long], ratingsForBlock: Array[BlockRating]) {

    override def toString: String = {
      s"InBlockInformation:((${elementIDs.mkString(",")}), (${ratingsForBlock.mkString("\n")}))"
    }
  }

  case class BlockRating(var ratings: Array[(Array[Int], Array[Double])]) {
    def apply(idx: Int) = ratings(idx)

    override def toString: String = {
      ratings.map {
        case (left, right) => s"((${left.mkString(",")}),(${right.mkString(",")}))"
      }.mkString(",")
    }
  }

  case class BlockedFactorization(userFactors: DataSet[(Int, Array[Array[Double]])],
                                  itemFactors: DataSet[(Int, Array[Array[Double]])])

  class BlockIDPartitioner extends FlinkPartitioner[Int] {
    override def partition(blockID: Int, numberOfPartitions: Int): Int = {
      blockID % numberOfPartitions
    }
  }

  class BlockIDGenerator(blocks: Int) extends Serializable {
    def apply(id: Long): Int = {
      (id % blocks).toInt
    }
  }

  // ================================= Factory methods =============================================

  def apply(): ALS = {
    new ALS()
  }

  // ===================================== Operations ==============================================

  /** Predict operation which calculates the matrix entry for the given indices  */
  implicit val predictRating = new PredictDataSetOperation[
      ALS,
      (Long, Long),
      (Long, Long, Double)] {
    override def predictDataSet(
        instance: ALS,
        predictParameters: ParameterMap,
        input: DataSet[(Long, Long)])
      : DataSet[(Long, Long, Double)] = {

      instance.factorsOption match {
        case Some((userFactors, itemFactors)) => {
          input.join(userFactors, JoinHint.REPARTITION_HASH_SECOND).where(0).equalTo(0)
            .join(itemFactors, JoinHint.REPARTITION_HASH_SECOND).where("_1._2").equalTo(0).map {
            triple => {
              val (((uID, iID), uFactors), iFactors) = triple

              val uFactorsVector = uFactors.factors
              val iFactorsVector = iFactors.factors

              val prediction = blas.ddot(
                uFactorsVector.length,
                uFactorsVector,
                1,
                iFactorsVector,
                1)

              (uID, iID, prediction)
            }
          }
        }

        case None => throw new RuntimeException("The ALS model has not been fitted to data. " +
          "Prior to predicting values, it has to be trained on data.")
      }
    }
  }

  implicit val predictRatingInt = new PredictDataSetOperation[ALS, (Int, Int), (Int, Int, Double)] {
    override def predictDataSet(
      instance: ALS,
      predictParameters: ParameterMap,
      input: DataSet[(Int, Int)])
    : DataSet[(Int, Int, Double)] = {
      val longInput = input.map { x => (x._1.toLong, x._2.toLong)}

      val longResult = implicitly[PredictDataSetOperation[ALS, (Long, Long), (Long, Long, Double)]]
        .predictDataSet(
          instance,
          predictParameters,
          longInput)

      longResult.map{ x => (x._1.toInt, x._2.toInt, x._3)}
    }
  }

  /** Calculates the matrix factorization for the given ratings. A rating is defined as
    * a tuple of user ID, item ID and the corresponding rating.
    *
    * @return Factorization containing the user and item matrix
    */
  implicit val fitALS =  new FitOperation[ALS, (Long, Long, Double)] {
    override def fit(
        instance: ALS,
        fitParameters: ParameterMap,
        input: DataSet[(Long, Long, Double)])
      : Unit = {
      val resultParameters = instance.parameters ++ fitParameters

      val userBlocks = resultParameters.get(Blocks).getOrElse(input.count.toInt)
      val itemBlocks = userBlocks
      val persistencePath = resultParameters.get(TemporaryPath)
      val seed = resultParameters(Seed)
      val factors = resultParameters(NumFactors)
      val iterations = resultParameters(Iterations)
      val lambda = resultParameters(Lambda)

      val ratings = input.map {
        entry => {
          val (userID, itemID, rating) = entry
          Rating(userID, itemID, rating)
        }
      }

      val blockIDPartitioner = new BlockIDPartitioner()

      val ratingsByUserBlock = ratings.map{
        rating =>
          val blockID = (rating.user % userBlocks).toInt
          (blockID, rating)
      } partitionCustom(blockIDPartitioner, 0)

      val ratingsByItemBlock = ratings map {
        rating =>
          val blockID = (rating.item % itemBlocks).toInt
          (blockID, new Rating(rating.item, rating.user, rating.rating))
      } partitionCustom(blockIDPartitioner, 0)

      val (uIn, uOut) = createBlockInformation(userBlocks, itemBlocks, ratingsByUserBlock,
        blockIDPartitioner)
      val (iIn, iOut) = createBlockInformation(itemBlocks, userBlocks, ratingsByItemBlock,
        blockIDPartitioner)

      val (userIn, userOut) = persistencePath match {
        case Some(path) => FlinkMLTools.persist(uIn, uOut, path + "userIn", path + "userOut")
        case None => (uIn, uOut)
      }

      val (itemIn, itemOut) = persistencePath match {
        case Some(path) => FlinkMLTools.persist(iIn, iOut, path + "itemIn", path + "itemOut")
        case None => (iIn, iOut)
      }

      val initialItems = itemOut.partitionCustom(blockIDPartitioner, 0).map{
        outInfos =>
          val blockID = outInfos._1
          val infos = outInfos._2

          (blockID, infos.elementIDs.map{
            id =>
              val random = new Random(id ^ seed)
              randomFactors(factors, random)
          })
      }.withForwardedFields("0")

      // iteration to calculate the item matrix
      val items = initialItems.iterate(iterations) {
        items => {
          val users = updateFactors(userBlocks, items, itemOut, userIn, factors, lambda,
            blockIDPartitioner)
          updateFactors(itemBlocks, users, userOut, itemIn, factors, lambda, blockIDPartitioner)
        }
      }

      val pItems = persistencePath match {
        case Some(path) => FlinkMLTools.persist(items, path + "items")
        case None => items
      }

      // perform last half-step to calculate the user matrix
      val users = updateFactors(userBlocks, pItems, itemOut, userIn, factors, lambda,
        blockIDPartitioner)

      instance.factorsOption = Some((
        unblock(users, userOut, blockIDPartitioner),
        unblock(pItems, itemOut, blockIDPartitioner)))
    }
  }

  implicit val fitALSInt =  new FitOperation[ALS, (Int, Int, Double)] {
    override def fit(
      instance: ALS,
      fitParameters: ParameterMap,
      input: DataSet[(Int, Int, Double)])
    : Unit = {

      val longInput = input.map { x => (x._1.toLong, x._2.toLong, x._3)}

      implicitly[FitOperation[ALS, (Long, Long, Double)]].fit(instance, fitParameters, longInput)
    }
  }

  /** Calculates a single half step of the ALS optimization. The result is the new value for
    * either the user or item matrix, depending with which matrix the method was called.
    *
    * @param numUserBlocks Number of blocks in the respective dimension
    * @param items Fixed matrix value for the half step
    * @param itemOut Out information to know where to send the vectors
    * @param userIn In information for the cogroup step
    * @param factors Number of latent factors
    * @param lambda Regularization constant
    * @param blockIDPartitioner Custom Flink partitioner
    * @return New value for the optimized matrix (either user or item)
    */
  def updateFactors(numUserBlocks: Int,
    items: DataSet[(Int, Array[Array[Double]])],
    itemOut: DataSet[(Int, OutBlockInformation)],
    userIn: DataSet[(Int, InBlockInformation)],
    factors: Int,
    lambda: Double, blockIDPartitioner: FlinkPartitioner[Int]):
  DataSet[(Int, Array[Array[Double]])] = {
    // send the item vectors to the blocks whose users have rated the items
    val partialBlockMsgs = itemOut.join(items).where(0).equalTo(0).
      withPartitioner(blockIDPartitioner).apply {
      (left, right, col: Collector[(Int, Int, Array[Array[Double]])]) => {
        val blockID = left._1
        val outInfo = left._2
        val factors = right._2
        var userBlock = 0
        var itemIdx = 0

        while(userBlock < numUserBlocks){
          itemIdx = 0
          val buffer = new ArrayBuffer[Array[Double]]
          while(itemIdx < outInfo.elementIDs.length){
            if(outInfo.outLinks(userBlock)(itemIdx)){
              buffer += factors(itemIdx)
            }
            itemIdx += 1
          }

          if(buffer.nonEmpty){
            // send update message to userBlock
            col.collect(userBlock, blockID, buffer.toArray)
          }

          userBlock += 1
        }
      }
    }

    // collect the partial update messages and calculate for each user block the new user vectors
    partialBlockMsgs.coGroup(userIn).where(0).equalTo(0).sortFirstGroup(1, Order.ASCENDING).
      withPartitioner(blockIDPartitioner).apply{
      new CoGroupFunction[(Int, Int, Array[Array[Double]]), (Int,
        InBlockInformation), (Int, Array[Array[Double]])](){

        // in order to save space, store only the upper triangle of the XtX matrix
        val triangleSize = (factors*factors - factors)/2 + factors
        val matrix = Array.fill(triangleSize)(0.0)
        val fullMatrix = Array.fill(factors * factors)(0.0)
        val userXtX = new ArrayBuffer[Array[Double]]()
        val userXy = new ArrayBuffer[Array[Double]]()
        val numRatings = new ArrayBuffer[Int]()

        override def coGroup(left: lang.Iterable[(Int, Int, Array[Array[Double]])],
          right: lang.Iterable[(Int, InBlockInformation)],
          collector: Collector[(Int, Array[Array[Double]])]): Unit = {
          // there is only one InBlockInformation per user block
          val inInfo = right.iterator().next()._2
          val updates = left.iterator()

          val numUsers = inInfo.elementIDs.length
          var blockID = -1

          var i = 0

          // clear old matrices and allocate new ones
          val matricesToClear = if (numUsers > userXtX.length) {
            val oldLength = userXtX.length

            while(i < (numUsers - oldLength)) {
              userXtX += Array.fill(triangleSize)(0.0)
              userXy += Array.fill(factors)(0.0)
              numRatings.+=(0)

              i += 1
            }

            oldLength
          } else {
            numUsers
          }

          i = 0
          while(i  < matricesToClear){
            numRatings(i) = 0

            util.Arrays.fill(userXtX(i), 0.0)
            util.Arrays.fill(userXy(i), 0.0)

            i += 1
          }

          var itemBlock = 0

          // build XtX matrices and Xy vector
          while(updates.hasNext){
            val update = updates.next()
            val blockFactors = update._3
            blockID = update._1

            var p = 0
            while(p < blockFactors.length){
              val vector = blockFactors(p)

              outerProduct(vector, matrix, factors)

              val (users, ratings) = inInfo.ratingsForBlock(itemBlock)(p)

              var i = 0
              while (i < users.length) {
                numRatings(users(i)) += 1
                blas.daxpy(matrix.length, 1, matrix, 1, userXtX(users(i)), 1)
                blas.daxpy(vector.length, ratings(i), vector, 1, userXy(users(i)), 1)

                i += 1
              }
              p += 1
            }

            itemBlock += 1
          }

          val array = new Array[Array[Double]](numUsers)

          i = 0
          while(i < numUsers){
            generateFullMatrix(userXtX(i), fullMatrix, factors)

            var f = 0

            // add regularization constant
            while(f < factors){
              fullMatrix(f*factors + f) += lambda * numRatings(i)
              f += 1
            }

            // calculate new user vector
            val result = new intW(0)
            lapack.dposv("U", factors, 1, fullMatrix, factors , userXy(i), factors, result)
            array(i) = userXy(i)

            i += 1
          }

          collector.collect((blockID, array))
        }
      }
    }.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0")
  }

  /** Creates the meta information needed to route the item and user vectors to the respective user
    * and item blocks.
    * * @param userBlocks
    * @param itemBlocks
    * @param ratings
    * @param blockIDPartitioner
    * @return
    */
  def createBlockInformation(userBlocks: Int, itemBlocks: Int, ratings: DataSet[(Int, Rating)],
    blockIDPartitioner: BlockIDPartitioner):
  (DataSet[(Int, InBlockInformation)], DataSet[(Int, OutBlockInformation)]) = {
    val blockIDGenerator = new BlockIDGenerator(itemBlocks)

    val usersPerBlock = createUsersPerBlock(ratings)

    val outBlockInfos = createOutBlockInformation(ratings, usersPerBlock, itemBlocks,
      blockIDGenerator)

    val inBlockInfos = createInBlockInformation(ratings, usersPerBlock, blockIDGenerator)

    (inBlockInfos, outBlockInfos)
  }

  /** Calculates the userIDs in ascending order of each user block
    *
    * @param ratings
    * @return
    */
  def createUsersPerBlock(ratings: DataSet[(Int, Rating)]): DataSet[(Int, Array[Long])] = {
    ratings.map{ x => (x._1, x._2.user)}.withForwardedFields("0").groupBy(0).
      sortGroup(1, Order.ASCENDING).reduceGroup {
      users => {
        val result = ArrayBuffer[Long]()
        var id = -1
        var oldUser = -1L

        while(users.hasNext) {
          val user = users.next()

          id = user._1

          if (user._2 != oldUser) {
            result.+=(user._2)
            oldUser = user._2
          }
        }

        val userIDs = result.toArray
        (id, userIDs)
      }
    }.withForwardedFields("0")
  }

  /** Creates the outgoing block information
    *
    * Creates for every user block the outgoing block information. The out block information
    * contains for every item block a [[scala.collection.mutable.BitSet]] which indicates which
    * user vector has to be sent to this block. If a vector v has to be sent to a block b, then
    * bitsets(b)'s bit v is set to 1, otherwise 0. Additionally the user IDataSet are replaced by
    * the user vector's index value.
    *
    * @param ratings
    * @param usersPerBlock
    * @param itemBlocks
    * @param blockIDGenerator
    * @return
    */
  def createOutBlockInformation(ratings: DataSet[(Int, Rating)],
    usersPerBlock: DataSet[(Int, Array[Long])],
    itemBlocks: Int, blockIDGenerator: BlockIDGenerator):
  DataSet[(Int, OutBlockInformation)] = {
    ratings.coGroup(usersPerBlock).where(0).equalTo(0).apply {
      (ratings, users) =>
        val userIDs = users.next()._2
        val numUsers = userIDs.length

        val userIDToPos = userIDs.zipWithIndex.toMap

        val shouldDataSend = Array.fill(itemBlocks)(new scala.collection.mutable.BitSet(numUsers))
        var blockID = -1
        while (ratings.hasNext) {
          val r = ratings.next()

          val pos =
            try {
              userIDToPos(r._2.user)
            }catch{
              case e: NoSuchElementException =>
                throw new RuntimeException(s"Key ${r._2.user} not  found. BlockID $blockID. " +
                  s"Elements in block ${userIDs.take(5).mkString(", ")}. " +
                  s"UserIDList contains ${userIDs.contains(r._2.user)}.", e)
            }

          blockID = r._1
          shouldDataSend(blockIDGenerator(r._2.item))(pos) = true
        }

        (blockID, OutBlockInformation(userIDs, new OutLinks(shouldDataSend)))
    }.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0")
  }

  /** Creates the incoming block information
    *
    * Creates for every user block the incoming block information. The incoming block information
    * contains the userIDs of the users in the respective block and for every item block a
    * BlockRating instance. The BlockRating instance describes for every incoming set of item
    * vectors of an item block, which user rated these items and what the rating was. For that
    * purpose it contains for every incoming item vector a tuple of an id array us and a rating
    * array rs. The array us contains the indices of the users having rated the respective
    * item vector with the ratings in rs.
    *
    * @param ratings
    * @param usersPerBlock
    * @param blockIDGenerator
    * @return
    */
  def createInBlockInformation(ratings: DataSet[(Int, Rating)],
    usersPerBlock: DataSet[(Int, Array[Long])],
    blockIDGenerator: BlockIDGenerator):
  DataSet[(Int, InBlockInformation)] = {
    // Group for every user block the users which have rated the same item and collect their ratings
    val partialInInfos = ratings.map { x => (x._1, x._2.item, x._2.user, x._2.rating)}
      .withForwardedFields("0").groupBy(0, 1).reduceGroup {
      x =>
        var userBlockID = -1
        var itemID = -1L
        val userIDs = ArrayBuffer[Long]()
        val ratings = ArrayBuffer[Double]()

        while (x.hasNext) {
          val (uBlockID, item, user, rating) = x.next
          userBlockID = uBlockID
          itemID = item

          userIDs += user
          ratings += rating
        }

        (userBlockID, blockIDGenerator(itemID), itemID, (userIDs.toArray, ratings.toArray))
    }.withForwardedFields("0")

    // Aggregate all ratings for items belonging to the same item block. Sort ascending with
    // respect to the itemID, because later the item vectors of the update message are sorted
    // accordingly.
    val collectedPartialInfos = partialInInfos.groupBy(0, 1).sortGroup(2, Order.ASCENDING).
      reduceGroup {
      new GroupReduceFunction[(Int, Int, Long, (Array[Long], Array[Double])), (Int,
        Int, Array[(Array[Long], Array[Double])])](){
        val buffer = new ArrayBuffer[(Array[Long], Array[Double])]

        override def reduce(iterable: lang.Iterable[(Int, Int, Long, (Array[Long],
          Array[Double]))], collector: Collector[(Int, Int, Array[(Array[Long],
          Array[Double])])]): Unit = {

          val infos = iterable.iterator()
          var counter = 0

          var blockID = -1
          var itemBlockID = -1

          while (infos.hasNext && counter < buffer.length) {
            val info = infos.next()
            blockID = info._1
            itemBlockID = info._2

            buffer(counter) = info._4

            counter += 1
          }

          while (infos.hasNext) {
            val info = infos.next()
            blockID = info._1
            itemBlockID = info._2

            buffer += info._4

            counter += 1
          }

          val array = new Array[(Array[Long], Array[Double])](counter)

          buffer.copyToArray(array)

          collector.collect((blockID, itemBlockID, array))
        }
      }
    }.withForwardedFields("0", "1")

    // Aggregate all item block ratings with respect to their user block ID. Sort the blocks with
    // respect to their itemBlockID, because the block update messages are sorted the same way
    collectedPartialInfos.coGroup(usersPerBlock).where(0).equalTo(0).
      sortFirstGroup(1, Order.ASCENDING).apply{
      new CoGroupFunction[(Int, Int, Array[(Array[Long], Array[Double])]),
        (Int, Array[Long]), (Int, InBlockInformation)] {
        val buffer = ArrayBuffer[BlockRating]()

        override def coGroup(partialInfosIterable:
        lang.Iterable[(Int, Int,  Array[(Array[Long], Array[Double])])],
          userIterable: lang.Iterable[(Int, Array[Long])],
          collector: Collector[(Int, InBlockInformation)]): Unit = {

          val users = userIterable.iterator()
          val partialInfos = partialInfosIterable.iterator()

          val userWrapper = users.next()
          val id = userWrapper._1
          val userIDs = userWrapper._2
          val userIDToPos = userIDs.zipWithIndex.toMap

          var counter = 0

          while (partialInfos.hasNext && counter < buffer.length) {
            val partialInfo = partialInfos.next()
            // entry contains the ratings and userIDs of a complete item block
            val entry = partialInfo._3

            val blockRelativeIndicesRatings = new Array[(Array[Int], Array[Double])](entry.size)

            // transform userIDs to positional indices
            for (row <- 0 until entry.length) {
              val rowEntries = entry(row)._1
              val rowIndices = new Array[Int](rowEntries.length)

              for (col <- 0 until rowEntries.length) {
                rowIndices(col) = userIDToPos(rowEntries(col))
              }

              blockRelativeIndicesRatings(row) = (rowIndices, entry(row)._2)
            }

            buffer(counter).ratings = blockRelativeIndicesRatings

            counter += 1
          }

          while (partialInfos.hasNext) {
            val partialInfo = partialInfos.next()
            // entry contains the ratings and userIDs of a complete item block
            val entry = partialInfo._3
            val blockRelativeIndicesRatings = new Array[(Array[Int], Array[Double])](entry.size)

            // transform userIDs to positional indices
            for (row <- 0 until entry.length) {
              val rowEntries = entry(row)._1
              val rowIndices = new Array[Int](rowEntries.length)

              for (col <- 0 until rowEntries.length) {
                rowIndices(col) = userIDToPos(rowEntries(col))
              }

              blockRelativeIndicesRatings(row) = (rowIndices, entry(row)._2)
            }

            buffer += new BlockRating(blockRelativeIndicesRatings)

            counter += 1
          }

          val array = new Array[BlockRating](counter)

          buffer.copyToArray(array)

          collector.collect((id, InBlockInformation(userIDs, array)))
        }
      }
    }.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0")
  }

  /** Unblocks the blocked user and item matrix representation so that it is at DataSet of
    * column vectors.
    *
    * @param users
    * @param outInfo
    * @param blockIDPartitioner
    * @return
    */
  def unblock(users: DataSet[(Int, Array[Array[Double]])],
    outInfo: DataSet[(Int, OutBlockInformation)],
    blockIDPartitioner: BlockIDPartitioner): DataSet[Factors] = {
    users.join(outInfo).where(0).equalTo(0).withPartitioner(blockIDPartitioner).apply {
      (left, right, col: Collector[Factors]) => {
        val outInfo = right._2
        val factors = left._2

        for(i <- 0 until outInfo.elementIDs.length){
          val id = outInfo.elementIDs(i)
          val factorVector = factors(i)
          col.collect(Factors(id, factorVector))
        }
      }
    }
  }

  // ================================ Math helper functions ========================================

  def outerProduct(vector: Array[Double], matrix: Array[Double], factors: Int): Unit = {
    var row = 0
    var pos = 0
    while(row < factors){
      var col = 0
      while(col <= row){
        matrix(pos) = vector(row) * vector(col)
        col += 1
        pos += 1
      }

      row += 1
    }
  }

  def generateFullMatrix(triangularMatrix: Array[Double], fullMatrix: Array[Double],
    factors: Int): Unit = {
    var row = 0
    var pos = 0

    while(row < factors){
      var col = 0
      while(col < row){
        fullMatrix(row*factors + col) = triangularMatrix(pos)
        fullMatrix(col*factors + row) = triangularMatrix(pos)

        pos += 1
        col += 1
      }

      fullMatrix(row*factors + row) = triangularMatrix(pos)

      pos += 1
      row += 1
    }
  }

  def generateRandomMatrix(users: DataSet[Int], factors: Int, seed: Long): DataSet[Factors] = {
    users map {
      id =>{
        val random = new Random(id ^ seed)
        Factors(id, randomFactors(factors, random))
      }
    }
  }

  def randomFactors(factors: Int, random: Random): Array[Double] = {
    Array.fill(factors)(random.nextDouble())
  }
}
