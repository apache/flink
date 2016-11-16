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

import java.lang.Iterable

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.flink.api.common.functions.{RichCoGroupFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common._
import org.apache.flink.ml.optimization.LearningRateMethod.{Default, LearningRateMethodTrait}
import org.apache.flink.ml.pipeline.FitOperation
import org.apache.flink.util.Collector

import scala.util.Random
import scala.collection.JavaConverters._

/** Alternating least squares algorithm to calculate a matrix factorization.
  *
  * Given a matrix `R`, SGD calculates two matrices `U` and `V` such that `R ~~ U^TV`. The
  * unknown row dimension is given by the number of latent factors. Since matrix factorization
  * is often used in the context of recommendation, we'll call the first matrix the user and the
  * second matrix the item matrix. The `i`th column of the user matrix is `u_i` and the `i`th
  * column of the item matrix is `v_i`. The matrix `R` is called the ratings matrix and
  * `(R)_{i,j} = r_{i,j}`.
  *
  * In order to find the user and item matrix, the following problem is solved:
  *
  * `argmin_{U,V} sum_(i,j\ with\ r_{i,j} != 0) (r_{i,j} - u_{i}^Tv_{j})^2 +
  * lambda (sum_(i) ||u_i||^2 + sum_(j) ||v_j||^2)`
  *
  * with `\lambda` being the regularization factor. This is the L2 regularization scheme to avoid
  * overfitting. Details can be found in the work of
  * [[http://dx.doi.org/10.1007/s10115-013-0682-2 Yu et al.]]
  *
  * We randomly create user and item vectors, then randomly partition them into `k` user
  * and `k` item blocks (`k` can be set by
  * [[org.apache.flink.ml.recommendation.MatrixFactorization.Blocks]]).
  *
  * Based on these factor blocks we partition the rating matrix to `k * k` blocks correspondingly.
  *
  * In one Flink iteration step we choose `k` non-conflicting rating blocks,
  * i.e. we should not choose two rating blocks simultaneously with the same user or item block.
  *
  * E.g. three different choices for `k = 3`
  *
  *  --------      --------      --------
  * |xx|  |  |    |  |  |xx|    |  |xx|  |
  * |--------|    |--------|    |--------|
  * |  |xx|  |    |xx|  |  |    |  |  |xx|
  * |--------|    |--------|    |--------|
  * |  |  |xx|    |  |xx|  |    |xx|  |  |
  *  --------      --------      --------
  *
  * Choosing such blocks is done by assigning a rating block ID to every user and item block.
  * We match the user, item, and rating blocks by the current rating block ID,
  * and update the user and item factors by gradients calculated locally from the ratings and
  * the corresponding user and item factors. This update is the logic of SGD.
  * We also update the rating block ID for the factor blocks, thus in the next iteration we use
  * other rating blocks to update the factors.
  *
  * In `k` Flink iteration we sweep through the whole rating matrix of `k * k` blocks.
  * Thus, `k` Flink iterations correspond to 1 iteration that considers all data,
  * so instead of the given number of iteration steps
  * (set at [[org.apache.flink.ml.recommendation.MatrixFactorization.Iterations]])
  * we perform `k * numberOfIterationSteps` Flink iterations.
  *
  * The matrix `R` is given in its sparse representation as a tuple of `(i, j, r)` where `i` is the
  * row index, `j` is the column index and `r` is the matrix value at position `(i,j)`.
  *
  * @example
  *          {{{
  *             val inputDS: DataSet[(Int, Int, Double)] = env.readCsvFile[(Int, Int, Double)](
  *               pathToTrainingFile)
  *
  *             val sgd = SGDForMatrixFactorization()
  *               .setIterations(10)
  *               .setNumFactors(10)
  *               .setLearningRate(0.001)
  *
  *             sgd.fit(inputDS)
  *
  *             val data2Predict: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)](pathToData)
  *
  *             sgd.predict(data2Predict)
  *          }}}
  *
  * =Parameters=
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.NumFactors]]:
  *  The number of latent factors. It is the dimension of the calculated user and item vectors.
  *  (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.Lambda]]:
  *  Regularization factor. Tune this value in order to avoid overfitting/generalization.
  *  (Default value: '''1''')
  *
  *  - [[org.apache.flink.ml.regression.MultipleLinearRegression.Iterations]]:
  *  The number of iterations to perform. (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.Blocks]]:
  *  The number of blocks into which the user and item matrix a grouped. The fewer
  *  blocks one uses, the less data is sent redundantly. However, bigger blocks entail bigger
  *  update messages which have to be stored on the Heap. If the algorithm fails because of
  *  an OutOfMemoryException, then try to increase the number of blocks. (Default value: '''None''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.Seed]]:
  *  Random seed used to generate the initial item matrix for the algorithm.
  *  (Default value: '''0''')
  *
  *  - [[org.apache.flink.ml.recommendation.SGDforMatrixFactorization.LearningRate]]:
  *  Initial learning rate, the weight with which we substract the gradient from the factors.
  *  (Default value: '''0.001''')
  *
  *  - [[org.apache.flink.ml.recommendation.SGDforMatrixFactorization.LearningRateMethod]]:
  *  Determines the functional form of the effective learning rate, i.e. the method for dynamically
  *  changing the learning rate during the iterations. SGD can only reach convergence when the
  *  learning rate is decreasing to zero.
  *  (Default value: '''initialLearningRate / sqrt(currentIteration)''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.TemporaryPath]]:
  *  Path to a temporary directory into which intermediate results are stored. If
  *  this value is set, then the algorithm is split into three preprocessing steps, the SGD
  *  iteration and a post-processing step which unblocks the factor matrices. The result of the
  *  individual steps are stored in the specified directory. By splitting the algorithm into
  *  multiple smaller steps, Flink does not have to split the available memory amongst too many
  *  operators. This allows the system to process bigger individual messages and improves the
  *  overall performance.
  *  (Default value: '''None''')
  */
class SGDforMatrixFactorization extends MatrixFactorization[SGDforMatrixFactorization] {

  import SGDforMatrixFactorization._

  /** Sets the initial learning rate for the algorithm.
    *
    * @param learningRate
    * @return
    */
  def setLearningRate(learningRate: Double): SGDforMatrixFactorization = {
    parameters.add(LearningRate, learningRate)
    this
  }

  /** Sets the method for gradually decreasing the learning rate.
    *
    * @param learningRateMethod
    * @return
    */
  def setLearningRateMethod(learningRateMethod: LearningRateMethodTrait):
  SGDforMatrixFactorization = {
    parameters.add(LearningRateMethod, learningRateMethod)
    this
  }
}

object SGDforMatrixFactorization {

  import MatrixFactorization._

  // ========================================= Parameters ==========================================

  case object LearningRate extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(0.001)
  }

  case object LearningRateMethod extends Parameter[LearningRateMethodTrait] {
    val defaultValue: Option[LearningRateMethodTrait] = Some(Default)
  }

  // ==================================== SGD type definitions =====================================

  /**
    * Index of a factor in a factor block.
    */
  type IndexInFactorBlock = Int

  /**
    * Representation of a rating in a rating block.
    *
    * @param rating Value of rating.
    * @param userIdx Index of user vector in the corresponding user block.
    * @param itemIdx Index of item vector in the corresponding item block.
    */
  case class RatingInfo(rating: Double,
                        userIdx: IndexInFactorBlock,
                        itemIdx: IndexInFactorBlock)

  /**
    * Rating block identifier.
    */
  type RatingBlockId = Int

  /**
    * Factor block identifier.
    */
  type FactorBlockId = Int

  /**
    * Representation of a rating block.
    *
    * @param id Identifier of the block.
    * @param block Array containing the ratings corresponding to the block.
    */
  case class RatingBlock(id: RatingBlockId, block: Array[RatingInfo])

  /**
    * Representation of a factor block.
    *
    * @param factorBlockId Identifier of the block.
    * @param currentRatingBlock Id of the rating block with which we are currently computing the
    *                           gradients
    * @param isUser Boolean marking whether it's a user or item block.
    * @param factors Array containing the vectors corresponding to the block.
    * @param omegas Array containing the omegas for every factor,
    *               i.e. the number of occurrences of that factor in the ratings.
    */
  case class FactorBlock(factorBlockId: FactorBlockId,
                         currentRatingBlock: RatingBlockId,
                         isUser: Boolean,
                         factors: Array[Array[Double]],
                         omegas: Array[Int])

  /**
    * Information for unblocking the factors at the end of the algorithm.
    *
    * @param factorBlockId Id of the factor block.
    * @param factorIds Ids of the factors in the corresponding factor block.
    */
  case class UnblockInformation(factorBlockId: FactorBlockId, factorIds: Array[Int])

  // ================================= Factory methods =============================================

  def apply(): SGDforMatrixFactorization = {
    new SGDforMatrixFactorization()
  }

  // ===================================== Operations ==============================================

  /**
    * Unblocks the factors (either user or item).
    *
    * @return [[DataSet]] containing the factors.
    */
  def unblock(factorBlocks: DataSet[FactorBlock],
              unblockInfo: DataSet[UnblockInformation],
              isUser: Boolean): DataSet[Factors] = {
    factorBlocks
      .filter(i => i.isUser == isUser)
      .join(unblockInfo).where(_.factorBlockId).equalTo(_.factorBlockId)
      .flatMap(x => x match {
        case (FactorBlock(_, _, _, factorsInBlock, _), UnblockInformation(_, ids)) =>
          ids.zip(factorsInBlock).map(x => Factors(x._1, x._2))
      })
  }

  /** Calculates the matrix factorization for the given ratings. A rating is defined as
    * a tuple of user ID, item ID and the corresponding rating.
    *
    * @return Factorization containing the user and item matrix
    */
  implicit val fitSGD = new FitOperation[SGDforMatrixFactorization, (Int, Int, Double)] {
    override def fit(
                      instance: SGDforMatrixFactorization,
                      fitParameters: ParameterMap,
                      input: DataSet[(Int, Int, Double)])
    : Unit = {
      val resultParameters = instance.parameters ++ fitParameters

      val numBlocks = resultParameters.get(Blocks).getOrElse(1)
      val seed = resultParameters.get(Seed)
      val factors = resultParameters(NumFactors)
      val iterations = resultParameters(Iterations)
      val lambda = resultParameters(Lambda)
      val learningRate = resultParameters(LearningRate)
      val learningRateMethod = resultParameters(LearningRateMethod)
      val persistencePath = resultParameters.get(TemporaryPath)

      val ratings = input

      // Initializing user and item blocks.
      // We keep information about indices in the blocks so we can construct rating blocks
      // without having to refer to the user or item ids directly. This way we don't have to use
      // these ids during the iteration. Thus, for doing the unblocking at the end,
      // we need to store information about the user and item ids.
      val (initialUserBlocks, userIdxInBlock, userUnblockInfoUnpersisted) =
        initFactorBlockAndIndices(ratings.map(_._1), isUser = true, numBlocks, seed, factors)
      val (initialItemBlocks, itemIdxInBlock, itemUnblockInfoUnpersisted) =
        initFactorBlockAndIndices(ratings.map(_._2), isUser = false, numBlocks, seed, factors)

      val (userUnblockInfo, itemUnblockInfo) = persistencePath match {
        case Some(path) => FlinkMLTools.persist(
          userUnblockInfoUnpersisted, itemUnblockInfoUnpersisted,
          path + "userUnblockInfo", path + "itemUnblockInfo")
        case None => (userUnblockInfoUnpersisted, itemUnblockInfoUnpersisted)
      }

      // Constructing the rating blocks. There are numBlocks * numBlocks rating blocks.
      // todo maybe optimize 3-way join
      // todo is forwarded fields effective when using map instead of apply at the end of join?
      val ratingBlocksUnpersisted = ratings
        .join(userIdxInBlock).where(_._1).equalTo(0)
        .join(itemIdxInBlock).where(_._1._2).equalTo(0)
        // matching the indices in the factor blocks (user and item) with the ratings
        // and creating rating block ids
        .map(_ match {
          case (((user, item, rating), (_, userIdx, userBlockId)), (_, itemIdx, itemBlockId)) =>
            (toRatingBlockId(userBlockId, itemBlockId, numBlocks),
              RatingInfo(rating, userIdx, itemIdx),
              // todo maybe eliminate this last item, only needed for deterministic result
              (user, item))
        })
        .withForwardedFields("_1._2._2->_2.userIdx", "_2._2->_2.itemIdx", "_1._1._3->_2.rating",
        "_1._1._1->_3._1", "_1._1._2->_3._2")
        // grouping by the rating block ids and constructing rating blocks
        .groupBy(0)
        .reduceGroup {
          ratings =>
            val arr = seed match {
              // sorting is only needed for deterministic result, i.e. when the seed is set
              case Some(_) => ratings.toArray.sortBy(_._3)
              case None => ratings.toArray
            }
            val ratingBlockId = arr(0)._1
            val ratingInfos = arr.map(_._2)
            RatingBlock(ratingBlockId, ratingInfos)
        }
        .withForwardedFields("_1->id")

      val ratingBlocks = persistencePath match {
        case Some(path) => FlinkMLTools.persist(ratingBlocksUnpersisted, path + "ratingBlocks")
        case None => ratingBlocksUnpersisted
      }

      // We union the user and item blocks so that we can use them as one [[DataSet]]
      // in the iteration.
      val initUserItem = initialUserBlocks.union(initialItemBlocks)

      // Iteratively updating the factors. We sweep through numBlocks rating blocks
      // in one iteration, thus we sweep through the whole rating matrix in numBlocks iterations.
      val userItemUnpersisted = initUserItem.iterate(iterations * numBlocks) {
        ui => updateFactors(ui, ratingBlocks, learningRate, learningRateMethod,
          lambda, numBlocks, seed)
      }

      val userItem = persistencePath match {
        case Some(path) => FlinkMLTools.persist(userItemUnpersisted, path + "userItem")
        case None => userItemUnpersisted
      }

      // unblocking the user and item matrices
      val users = unblock(userItem, userUnblockInfo, isUser = true)
      val items = unblock(userItem, itemUnblockInfo, isUser = false)

      instance.factorsOption = Some((users, items))
    }
  }

  /** Calculates a single sweep for the SGD optimization. The result is the new value for
    * the user and item matrix.
    *
    * @return New values for the optimized matrices.
    */
  def updateFactors(userItem: DataSet[FactorBlock],
                    ratingBlocks: DataSet[RatingBlock],
                    learningRate: Double,
                    learningRateMethod: LearningRateMethodTrait,
                    lambda: Double,
                    numBlocks: Int,
                    seed: Option[Long]): DataSet[FactorBlock] = {

    /**
      * Updates one user and item block based on one corresponding rating block.
      * This is the local logic of the SGD.
      *
      * @return The updated user and item block.
      */
    def updateLocalFactors(ratingBlock: RatingBlock,
                           userBlock: FactorBlock,
                           itemBlock: FactorBlock,
                           iteration: Int): (FactorBlock, FactorBlock) = {

      val effectiveLearningRate = learningRateMethod.calculateLearningRate(
        learningRate,
        iteration + 1,
        lambda)

      val RatingBlock(ratingBlockId, ratings) = ratingBlock
      val FactorBlock(_, _, _, users, userOmegas) = userBlock
      val FactorBlock(_, _, _, items, itemOmegas) = itemBlock

      val random = seed.map(s => new Random(iteration ^ ratingBlockId ^ s)).getOrElse(Random)
      val shuffleIdxs = random.shuffle[Int, IndexedSeq](ratings.indices)

      shuffleIdxs.map(ratingIdx => ratings(ratingIdx)) foreach {
        rating => {
          val pi = users(rating.userIdx)
          val omegai = userOmegas(rating.userIdx)

          val qj = items(rating.itemIdx)
          val omegaj = itemOmegas(rating.itemIdx)

          val rij = rating.rating

          val piqj = rij - blas.ddot(pi.length, pi, 1, qj, 1)

          val newPi = pi.zip(qj).map { case (p, q) =>
            p - effectiveLearningRate * (lambda / omegai * p - piqj * q) }
          val newQj = pi.zip(qj).map { case (p, q) =>
            q - effectiveLearningRate * (lambda / omegaj * q - piqj * p) }

          users(rating.userIdx) = newPi
          items(rating.itemIdx) = newQj
        }
      }

      (userBlock.copy(factors = users), itemBlock.copy(factors = items))
    }

    /**
      * Helper method for extracting the user and item block for a rating block.
      *
      * Because one of the two blocks might be missing, it returns [[Option]]s,
      * and also the rating block id, so we even if we cannot update the factors in the block,
      * we can update current rating block id in the factor blocks, preparing the next step.
      *
      * @return Optional user and item blocks and the current rating block id.
      */
    def extractUserItemBlock(factorBlocks: Iterator[FactorBlock]):
      (Option[FactorBlock], Option[FactorBlock], RatingBlockId) = {
      val b1 = factorBlocks.next()
      val b2 = factorBlocks.toIterable.headOption

      if (b1.isUser) {
        (Some(b1), b2, b1.currentRatingBlock)
      } else {
        (b2, Some(b1), b1.currentRatingBlock)
      }
    }

    // todo Consider left outer join.
    //   - pros
    //      . it would eliminate rating block check (no need to "invoke" unused rating block)
    //      . flexible underlying implementation (shipping/local strategy)
    //   - cons
    //      . has to aggregate the two factor blocks in the join function
    // Matching the user and item blocks to the current rating block.
    userItem.coGroup(ratingBlocks)
      .where(factorBlock => factorBlock.currentRatingBlock)
      .equalTo(ratingBlock => ratingBlock.id).apply(
      new RichCoGroupFunction[FactorBlock, RatingBlock, FactorBlock] {

        override def coGroup(factorBlocksJava: Iterable[FactorBlock],
                             ratingBlocksJava: Iterable[RatingBlock],
                             out: Collector[FactorBlock]): Unit = {

          val factorBlocks = factorBlocksJava.asScala.iterator
          val ratingBlocks = ratingBlocksJava.asScala.iterator

          // We only need to update factors by the current rating block
          // if there are factors matched to that rating block.
          if (factorBlocks.hasNext) {
            // There are factors matched to the current rating block,
            // so we are updating those factors by the current rating block.

            // There are two factor blocks, one user and one item.
            // One of them might be missing when there is no rating block,
            // so we use options.
            val (userBlock, itemBlock, currentRatingBlock) = extractUserItemBlock(factorBlocks)

            val (updatedUserBlock, updatedItemBlock) =
              if (ratingBlocks.hasNext) {
                // there is one rating block
                val ratingBlock = ratingBlocks.next()

                val currentIteration = getIterationRuntimeContext.getSuperstepNumber / numBlocks

                val (userB, itemB) =
                  updateLocalFactors(ratingBlock, userBlock.get, itemBlock.get, currentIteration)

                (Some(userB), Some(itemB))
              } else {
                // There are no ratings in the current block, so we do not update the factors,
                // just pass them forward.

                (userBlock, itemBlock)
              }

            // calculating the next rating block for the factor blocks
            val (newP, newQ) = nextRatingBlock(currentRatingBlock, numBlocks)

            updatedUserBlock.foreach(x => out.collect(x.copy(currentRatingBlock = newP)))
            updatedItemBlock.foreach(x => out.collect(x.copy(currentRatingBlock = newQ)))
          }
        }
      }).withForwardedFieldsFirst("factorBlockId", "isUser", "omegas")
  }

  // =============================== Block helper functions ========================================

  /**
    * Initializes blocks for one factor matrix (either user or item).
    *
    * @param factorIdsForRatings [[DataSet]] containing the ids of the factors.
    * @param isUser Indicates whether it's a user or item matrix.
    * @param numBlocks Number of matrix blocks.
    * @param seed Random seed
    * @param factors Number of factors.
    * @return Three [[DataSet]]s: the factor blocks,
    *         the factor ids matched to their index in the factor block,
    *         and the information for unblocking.
    */
  def initFactorBlockAndIndices(factorIdsForRatings: DataSet[Int],
                                isUser: Boolean,
                                numBlocks: Int,
                                seed: Option[Long],
                                factors: Int):
  (DataSet[FactorBlock], DataSet[(Int, Int, FactorBlockId)], DataSet[UnblockInformation]) = {

    val factorIDs = factorIdsForRatings.distinct()

    val factorBlockIds: DataSet[(Int, FactorBlockId)] = factorIDs
      .map(new RichMapFunction[Int, (Int, FactorBlockId)] {
        @transient
        var random: Random = _

        override def open(parameters: Configuration): Unit = {
          random = Random
        }

        override def map(id: Int): (Int, RatingBlockId) = {
          val currentRandom = seed.map(s => new Random(id ^ s)).getOrElse(random)
          (id, currentRandom.nextInt(numBlocks))
        }
      })

    val factorCounts = factorIdsForRatings
      .map((_, 1))
        .withForwardedFields("*->_1")
      .groupBy(0)
      .sum(1)

    val initialFactorBlocks =
      factorBlockIds
        .join(factorCounts).where(0).equalTo(0)
        .map(_ match {
          case ((id, factorBlockId), (_, count)) =>
            val random = seed.map(s => new Random(id ^ s)).getOrElse(Random)
            (factorBlockId, Factors(id, randomFactors(factors, random)), count)
        })
        .withForwardedFields("_1._1->_2.id", "_1._2->_1", "_2._2->_3")
        .groupBy(0).reduceGroup {
        users => {
          val arr = seed match {
            // sorting is only needed for deterministic result, i.e. when the seed is set
            case Some(_) => users.toArray.sortBy(_._2.id)
            case None => users.toArray
          }
          val factors = arr.map(_._2)
          val omegas = arr.map(_._3)
          val factorBlockId = arr(0)._1
          val initialRatingBlock = factorBlockId * (numBlocks + 1)

          val factorIds = factors.map(_.id)

          (FactorBlock(factorBlockId, initialRatingBlock, isUser, factors.map(_.factors), omegas),
            factorIds)
        }
      }.withForwardedFields("_1->_1.factorBlockId")

    val unblockInfo = initialFactorBlocks
      .map(x => UnblockInformation(x._1.factorBlockId, x._2))
      .withForwardedFields("_1.factorBlockId->factorBlockId", "_2->factorIds")

    val factorIdxInBlock = unblockInfo
      .flatMap {
        _ match {
          case UnblockInformation(factorBlockId, ids) =>
            ids.zipWithIndex.map {
              case (id, idx) =>
                (id, idx, factorBlockId)
            }
        }
      }
      .withForwardedFields("factorBlockId->_3")

    (initialFactorBlocks.map(_._1).withForwardedFields("_1->*"), factorIdxInBlock, unblockInfo)
  }

  // ================ Helper functions for matching rating and factor blocks =======================

  /**
    * Logic that calculates the rating block id based on the user and item block ids.
    *
    * @return Rating block id.
    */
  def toRatingBlockId(userBlockId: FactorBlockId,
                      itemBlockId: FactorBlockId,
                      numOfBlocks: Int): RatingBlockId = {
    userBlockId * numOfBlocks + itemBlockId
  }

  /**
    * Logic that creates the rating block id for the next iteration step,
    * returning the next rating block ids for the current user factor block and item factor block.
    *
    * @param currentRatingBlock Current rating block id.
    * @param numFactorBlocks Number of factor blocks.
    * @return The next rating block id for the user and item blocks.
    */
  def nextRatingBlock(currentRatingBlock: RatingBlockId,
                      numFactorBlocks: Int): (RatingBlockId, RatingBlockId) = {
    val pRow = currentRatingBlock / numFactorBlocks
    val qRow = currentRatingBlock % numFactorBlocks

    val newP = pRow * numFactorBlocks + (currentRatingBlock + 1) % numFactorBlocks
    val newQ = ((pRow + numFactorBlocks - 1) % numFactorBlocks) * numFactorBlocks + qRow
    (newP, newQ)
  }
}
