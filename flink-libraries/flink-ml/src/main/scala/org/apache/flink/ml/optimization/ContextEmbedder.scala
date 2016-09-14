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

package org.apache.flink.ml.optimization

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.flink.api.common._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.ml._
import org.apache.flink.ml.common.Parameter

import scala.collection.immutable.LongMap
import org.apache.flink.ml.optimization.Embedder._
import org.apache.flink.util.XORShiftRandom

import scala.reflect.ClassTag

/**
  * Constructs a huffman binary tree from a set of objects and frequencies
  *
  * used by the ContextEmbedder to encode unique elements and construct a
  * weight set corresponding to huffman tree traversal from root to a given element
  * this is used during training to reduce network passes:
  *   - naive is O(N&#94;2): needing to compute the likelihood of output probabilities
  *     for every vocabulary element given every other vocabulary element
  *   - using tree traversal is O(logN): only need to compute probabilities of descent from root
  *     to expected leaf of tree
  *
  * this tree is key to implementation of *hierarchical softmax* - a significant optimization of
  * backpropagation in systems predisposed to distribution along frequencies
  *
  * read more about HSM and binary trees in W2V here: https://arxiv.org/pdf/1411.2738v4.pdf
  */

object HuffmanBinaryTree extends Serializable {
  import collection.mutable

  abstract class Tree[+A]

  private type WeightedNode = (Tree[Any], Int)

  implicit def orderByInverseWeight[A <: WeightedNode]: Ordering[A] =
    Ordering.by {
      case (_, weight) => -1 * weight
    }

  //by leaning on typing of leafs and branches, we can match on type to exclude
  //much of the usual recursive edge case checking
  private case class Leaf[A](value: A) extends Tree[A]

  private case class Branch[A](left: Tree[A], right: Tree[A]) extends Tree[A]

  /** Removes values from priority queue ordered by inverted weight,
    * places values in branch of a tree, weights the branch by summing component weights,
    * and adds back to queue until all values have been inserted into tree
    *
    * @param xs [[scala.collection.mutable.PriorityQueue[WeightedNode]]]
    * @return [[scala.collection.mutable.PriorityQueue[WeightedNode]]] with single member
    */
  private def merge(xs: mutable.PriorityQueue[WeightedNode])
  : mutable.PriorityQueue[WeightedNode] = {
    if (xs.length == 1) xs
    else {
      val l = xs.dequeue
      val r = xs.dequeue
      val merged = (Branch(l._1, r._1), l._2 + r._2)
      merge(xs += merged)
    }
  }

  /** Convenience for building and extracting from priority queue
    *
    * @param xs [[scala.collection.Iterable[WeightedNode]]]
    * @return [[scala.collection.immutable.List[WeigthedNode]]]
    */
  private def merge(xs: Iterable[WeightedNode]): Iterable[WeightedNode] = {
    //form priority queue, build tree, return a list (should have only 1 member)
    merge(new mutable.PriorityQueue[WeightedNode] ++= xs).toList
  }

  /** recursively search the branches of a supplied tree for the required value
    *
    * @param tree [[org.apache.flink.ml.optimization.HuffmanBinaryTree]]
    * @param value [[Any]]
    * @return [[Boolean]]
    */
  private def contains(tree: Tree[Any], value: Any): Boolean = tree match {
    case Leaf(c) => if (c == value) true else false
    case Branch(l, r) => contains(l, value) || contains(r, value)
  }

  /** recursively build the huffman encoding of a token value
    * the code - returned as a vector of integers, represents the sequence
    * of left (0) or right (1) turns needed to arrive at a leaf from the root
    * @param tree [[org.apache.flink.ml.optimization.HuffmanBinaryTree]]
    * @param value [[Any]]
    * @return [[scala.collection.immutable.Vector[Int]]]
    */
  def encode(tree: Tree[Any], value: Any): Vector[Int] = {
    def turn(tree: Tree[Any], value: Any,
             code: Vector[Int]): Vector[Int] = tree match {
      case Leaf(_) => code
      case Branch(l, r) =>
        (contains(l, value)) match {
          case true => turn(l, value, code :+ 0)
          case _ => turn(r, value, code :+ 1)
        }
    }
    turn(tree, value, Vector.empty[Int])
  }

  /** takes a code and decomposes into visited node identities
    * for a given code, we return the series of inner node codes passed through
    * this is used to map traversals to vectors during HSM training
    *
    * @param code [[scala.collection.immutable.Vector[Int]]]
    * @return [[scala.collection.immutable.Vector[String]]]
    */
  def path(code: Vector[Int]) : Vector[String] = {
    def form(code: Vector[Int], path: Vector[String]): Vector[String] = code match {
      case _ +: IndexedSeq() => path.reverse
      case head +: tail => path match {
        case _ +: IndexedSeq() => form(tail, head.toString +: path)
        case _ => form(tail, (path.head + head) +: path)
      }
    }
    form(code, Vector("root"))
  }

  /** given a set of values and integer weights, constructs a huffman binary tree
    * to be used for encoding
    *
    * @param weightedLexicon [[scala.collection.Iterable[(Any, Int)]]]
    * @return [[org.apache.flink.ml.optimization.HuffmanBinaryTree.Tree]]
    */
  def tree(weightedLexicon: Iterable[(Any, Int)]): Tree[Any] =
    merge(weightedLexicon.map(x => (Leaf(x._1), x._2))).head._1
}

trait WeightMatrix

trait TrainingSet

/** case class used to map a target value to a set of contextual values.
  * in the case of a SkipGram architecture for the context embedder - this
  * is to say: given a target value, these values are expected to appear as context.
  * this gives the 'ground' we train against, as the network guesses context for targets
  *
  * @param target the target value - this will be decomposed into a
  *               huffman tree path later in training
  * @param context the set of context values - these will be mapped directly to numeric vectors
  * @tparam T the type of values being embedded - i.e. String for Word2Vec
  */
case class Context[T](target: T, context: Iterable[T])

case class HSMTargetValue(index: Long, code: Vector[Int], path: Vector[String])

case class HSMStepValue(innerIndex: Vector[Long], code: Vector[Int], codeDepth: Int)

case class HSMTrainingSet(leafSet: Vector[Long], innerSet: HSMStepValue) extends TrainingSet

/** the return type for ContextEmbedder optimizations - gives mapping of target values to
  * huffman encodings, paths and vector indices,
  * mappings of huffman node identities to vector indices,
  * target value index to vector and huffman value index to vector
  *
  * @param leafMap [[scala.collection.immutable.Map[T, HSMTargetValue]]]
  * @param innerMap [[scala.collection.immutable.Map[String, Long]]]
  * @param leafVectors [[scala.collection.immutable.LongMap[Array[Double]]]
  * @param innerVectors [[scala.collection.immutable.LongMap[Array[Double]]]
  * @tparam T the type of values being embedded - i.e. String for Word2Vec
  */
case class HSMWeightMatrix[T](leafMap: Map[T, HSMTargetValue],
                              innerMap: Map[String, Long],
                              leafVectors: LongMap[Array[Double]],
                              innerVectors: LongMap[Array[Double]]) extends WeightMatrix {

  /** concatenation method for combining two instances of this case class elementwise
    *
    * @param that [[org.apache.flink.ml.optimization.HSMWeightMatrix]]
    * @return [[org.apache.flink.ml.optimization.HSMWeightMatrix]]
    */
  def ++ (that: HSMWeightMatrix[T])
  : HSMWeightMatrix[T] = {
    HSMWeightMatrix(
      this.leafMap ++ that.leafMap,
      this.innerMap ++ that.innerMap,
      this.leafVectors ++ that.leafVectors,
      this.innerVectors ++ that.innerVectors)
  }

  /** takes a tuple of LongMap[Array[Double and composes them into the given HSMWeightMatrix
    * overwriting the existing vectors with any updated vectors
    *
    * @param weights
    * @return [[org.apache.flink.ml.optimization.HSMWeightMatrix]]
    */
  def updateVectors(weights: (LongMap[Array[Double]], LongMap[Array[Double]]))
  : HSMWeightMatrix[T] = {
    this.copy(leafVectors =
      this.leafVectors ++ weights._1, innerVectors = this.innerVectors ++ weights._2)
  }

  /** zips the target values to corresponding vectors
    *
    * @return
    */
  def fetchVectors: Map[T, Vector[Double]] = {
    this.leafMap.map(x => x._1 -> this.leafVectors.getOrElse(x._2.index, Array.empty).toVector)
  }
}

/**
  * Implements a generic Word2Vec; an object-embedding algorithm first described
  * by Tomáš Mikolov et al in http://arxiv.org/pdf/1301.3781.pdf
  *
  * this implementation is designed to handle data of arbitrary type that meets some abstract
  * criteria of being contextual (i.e. can be placed into Context[T]) and classifiable
  * (i.e. a Huffman encoder can be built around a simple frequency count)
  *
  * Hierarchical SoftMax is an approach to the softmax classification solver
  * that utilizes a distributed, sequential representation of class output probabilities
  * via a huffman encoding of known classes and a training process that 'learns'
  * the output classes by traversing the inner probabilities of the encoding
  *
  * More on hierarchical softmax: (http://www-personal.umich.edu/~ronxin/pdf/w2vexp.pdf)
  * More on softmax: (https://en.wikipedia.org/wiki/Softmax_function)
  */

object Embedder {
  val MIN_LEARNING_RATE = 0.0001

  case object Iterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }

  case object TargetCount extends Parameter[Int] {
    val defaultValue = Some(5)
  }

  case object VectorSize extends Parameter[Int] {
    val defaultValue = Some(100)
  }

  case object LearningRate extends Parameter[Double] {
    val defaultValue = Some(0.015)
  }

  case object Seed extends Parameter[Long] {
    val defaultValue = Some(scala.util.Random.nextLong)
  }

  case object BatchSize extends Parameter[Int] {
    val defaultValue = Some(1000)
  }

}

/** Embedder carries the general parameters for embedders
  *
  * @tparam A type holding embedding candidates
  * @tparam B type holding the embeddings
  */
abstract class Embedder[A, B] extends Solver[A, B] {
  import Embedder._
  def setIterations(iterations: Int): this.type = {
    parameters.add(Iterations, iterations)
    this
  }

  def setTargetCount(targetCount: Int): this.type = {
    parameters.add(TargetCount, targetCount)
    this
  }

  def setVectorSize(vectorSize: Int): this.type = {
    parameters.add(VectorSize, vectorSize)
    this
  }

  def setLearningRate(learningRate: Double): this.type = {
    parameters.add(LearningRate, learningRate)
    this
  }

  def setBatchSize(batchSize: Int): this.type = {
    parameters.add(BatchSize, batchSize)
    this
  }

  def setSeed(seed: Long): this.type = {
    parameters.add(Seed, seed)
    this
  }
}

/** implements a generic context embedding learner -
  * calculates a set of learned vectors representing the context embedding of elements of type T
  * @example
  * {{{
  *   val contextDS: DataSet[Context[T]] =  ...
  *
  *   val contextEmbedder : ContextEmbedder[T] =
  *     new ContextEmbedder()
  *       .setIterations(5)
  *       .setVectorSize(50)
  *       .setBatchSize(500)
  *
  *   val initialWeightsDS : DataSet[HSMWeightMatrix] = contextEmbedder.fit(contextDS)
  *
  *   val learnedWeightsDS : DataSet[HSMWeightMatrix] = contextEmbedder.optimize(contextDS)
  *
  *   val learnedVectors = learnedWeightsDS.map(_.fetchVectors)
  * }}}
  *
  * =Parameters=
  *
  * - [[org.apache.flink.ml.optimization.Embedder.Iterations]]
  * sets the number of global iterations the training set is passed through -
  * essentially looping on whole set, leveraging flink's iteration operator
  * (Default value: '''10''')
  *
  * - [[org.apache.flink.ml.optimization.Embedder.TargetCount]]
  * sets the minimum number of occurences of a given target value before that value
  * is excluded from vocabulary
  * (e.g. if this parameter is set to '5', and a target appears in the training set
  * fewer than 5 times, it is not included in vocabulary) (Default value: '''5''')
  *
  * - [[org.apache.flink.ml.optimization.Embedder.VectorSize]]
  * sets the length of each learned vector (Default value: '''100''')
  *
  * - [[org.apache.flink.ml.optimization.Embedder.LearningRate]]
  * sets the rate of descent during backpropagation - this value decays linearly with
  * individual training sets, determined by BatchSize (Default value: '''0.015''')
  *
  * - [[org.apache.flink.ml.optimization.Embedder.BatchSize]]
  * sets the batch size of training sets - the input DataSet will be batched into
  * groups of this size for learning (Default value: '''1000''')
  *
  * - [[org.apache.flink.ml.optimization.Embedder.Seed]]
  * sets the seed for generating random vectors at initial weighting DataSet creation
  * (Default value: '''Some(scala.util.Random.nextLong)''')
  *
  * @tparam T the type of values being embedded - i.e. String for Word2Vec
  */
class ContextEmbedder[T: ClassTag: typeinfo.TypeInformation]
  extends Embedder[Context[T], HSMWeightMatrix[T]] {

  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6

  def numberOfIterations: Int = parameters(Iterations)
  def minTargetCount: Int = parameters(TargetCount)
  def vectorSize: Int = parameters(VectorSize)
  def learningRate: Double = parameters(LearningRate)
  def batchSize: Int = parameters(BatchSize)
  def seed: Long = parameters(Seed)

  /** preprocesses a Context[T] set with a HSMWeightMatrix[T] and iterates result through
    * training and weigh updates
    *
    * @param data [[DataSet[Context[T]]]
    * @param initialWeights [[Option[HSMWeightMatrix[T]]]
    * @return [[DataSet[org.apache.flink.ml.optimization.HSMWeightMatrix[T]]]
    */
  def optimize(data: DataSet[Context[T]],
               initialWeights: Option[DataSet[HSMWeightMatrix[T]]]): DataSet[HSMWeightMatrix[T]] = {

    val weights: DataSet[HSMWeightMatrix[T]] = createInitialWeightsDS(initialWeights, data)

    val vocab = weights.map(x => x.leafMap.keySet)

    val vocabSize = vocab.flatMap(x => x).count().toInt

    val preparedData = data
      .filterWithBcVariable(vocab){
        (context, vocab) =>
          vocab.contains(context.target)
      }
      .mapWithBcVariable(weights){
        (context, weights) =>
          val target = weights.leafMap.get(context.target).get
          val path = target.path
          val code = target.code
          val codeDepth = code.size
          val contextIndices = context.context.flatMap(x =>
            weights.leafMap.get(x) match {
              case Some(value) => Some(value.index)
              case None => None
            }).to[Vector]
          val pathIndices = path.flatMap(x =>
            weights.innerMap.get(x) match {
              case Some(index) => Some(index)
              case None => None
            })
          HSMTrainingSet(contextIndices, HSMStepValue(pathIndices, code, codeDepth))
      }.filter(_.leafSet.nonEmpty)

    weights.iterate(numberOfIterations) {
      w => trainIteration(preparedData, w, vocabSize, learningRate)
    }
  }

  /** takes an optional initialWeight - can be extended to partially rebuild existing
    * vocabulary with new data, but for now just returns the original or builds an entirely
    * new dataset
    *
    * @param initialWeights optional existing weights set
    * @param data the dataset being optimized create weight set
    * @return a weights set for use in optimization
    */
  def createInitialWeightsDS(initialWeights: Option[DataSet[HSMWeightMatrix[T]]],
                              data: DataSet[Context[T]])
  : DataSet[HSMWeightMatrix[T]] = initialWeights match {
    case Some(weightMatrix) => weightMatrix
    case None => formHSMWeightMatrix(data)
  }

  /** Forms an HSMWeightMatrix[T] object from a DataSet[Context[T]]
    *
    * @param data [[DataSet[Context[T]]]
    * @return [[HSMWeightMatrix[T]]]
    */
  private def formHSMWeightMatrix(data: DataSet[Context[T]])
  : DataSet[HSMWeightMatrix[T]] = {
    val targets = data
      .map(x => (x.target, 1))
      .groupBy(0).sum(1)
      .filter(_._2 >= minTargetCount)

    val softMaxTree = HuffmanBinaryTree.tree(targets.collect())

    val leafValues = targets
      .map {
        target => {
          val code = HuffmanBinaryTree.encode(softMaxTree, target._1)
          val path = HuffmanBinaryTree.path(code)
          target._1 -> (code, path)
        }
      }.zipWithIndex

    val innerMap = leafValues
      .flatMap(x => x._2._2._2)
      .distinct()
      .zipWithIndex
      .map(x => Map(x._2 -> x._1) -> LongMap(x._1 -> new Array[Double](vectorSize)))
      .reduce((a,b) => (a._1 ++ b._1) -> (a._2 ++ b._2))
      .map(m => HSMWeightMatrix(Map.empty[T, HSMTargetValue], m._1, LongMap.empty, m._2))

    val initRandom = new XORShiftRandom(seed)

    val leafMap = leafValues
      .map(x => Map(x._2._1 -> HSMTargetValue(x._1, x._2._2._1, x._2._2._2)) ->
        LongMap(x._1 -> Array.fill[Double](vectorSize)(
        (initRandom.nextDouble() - 0.5f) / vectorSize)))
      .reduce((a,b) => (a._1 ++ b._1) -> (a._2 ++ b._2))
      .map(m => HSMWeightMatrix(m._1, Map.empty, m._2, LongMap.empty))

    innerMap.union(leafMap).reduce(_ ++ _)
  }

  /** takes preprocessed Data and a WeightSets, groups them into BatchSize
    * and trains vectors using Hierarchical SoftMax
    *
    * @param data [[DataSet[HSMTrainingSet]]
    * @param weights [[DataSet[HSMWeightMatrix[T]]]
    * @param vocabSize [[Int]]
    * @param learningRate [[Double]]
    * @return [[DataSet[HSMWeightMatrix[T]]]
    */
  private def trainIteration(data: DataSet[HSMTrainingSet],
                       weights: DataSet[HSMWeightMatrix[T]],
                       vocabSize: Int,
                       learningRate: Double)
  : DataSet[HSMWeightMatrix[T]] = {
    val learnedWeights = data
      .mapPartition(_.grouped(batchSize))
      .mapWithBcVariable(weights)(train)

    val learnedLeafWeights = aggregateWeights(
      learnedWeights.map(_._1).flatMap(x => x),
      vocabSize)

    val learnedInnerWeights = aggregateWeights(
      learnedWeights.map(_._2).flatMap(x => x),
      vocabSize)

    learnedLeafWeights
      .crossWithTiny(learnedInnerWeights)
      .crossWithTiny(weights)
      .map(x => x._2.updateVectors(x._1))
  }

  /** aggregates sparse weight sets returned by training into a single map of learned weight values
    *
    * @param weights
    * @param vocabSize
    * @return
    */
  private def aggregateWeights(weights: DataSet[(Long, Array[Double])], vocabSize: Int)
  : DataSet[LongMap[Array[Double]]] = weights
    .groupBy(0)
    .reduce(sumWeights(_,_))
    .map(x => LongMap.singleton(x._1, x._2))
    .reduce(_ ++ _)

  /** taking advantage of distributed representations to aggregate by summation
    * find more about this here: http://www.aclweb.org/anthology/W15-1513
    *
    * @param vecA
    * @param vecB
    * @tparam V
    * @return
    */
  private def sumWeights[V <: (Long, Array[Double])](vecA: V, vecB: V)  = {
    val targetVector = vecB._2
    blas.daxpy(vectorSize, 1.0d, vecA._2, 1, targetVector, 1)
    (vecB._1, targetVector)
  }

  /** given a grouped sequence of training sets:
    * passes forward over given vectors and backpropagates error to learn 'valid' embedded vectors
    *
    * @param context [[Seq[HSMTrainingSet]]]
    * @param weights [[(LongMap[Array[Double]], LongMap[Array[Double)]]
    * @param alpha [[Option[Double]]]
    * @return [[(Seq[(Long, Array[Double])], Seq[(Long, Array[Double])])]]
    */
  private def train(
    context: Seq[HSMTrainingSet],
    weights: (LongMap[Array[Double]], LongMap[Array[Double]]),
    alpha: Option[Double])
  : (Seq[(Long, Array[Double])], Seq[(Long, Array[Double])]) = {
    //initialize lookup table for non-linearity
    val expTable = createExpTable()

    val vocabSize = weights._1.size

    //dense representation of which indices from weights have been updated
    val leafModify = new Array[Int](vocabSize)
    val innerModify = new Array[Int](vocabSize)

    val initialAlpha = alpha.getOrElse(learningRate)

    val count = context.size

    //folds on context seq - allows us to develop weights across the entire sequence
    val model = context.foldLeft((weights._1, weights._2, initialAlpha, 0, count)) {
      case ((leafWeights, innerWeights, a, trainSetPos, trainSetSize), trainingSet) =>

        // decay the learning rate to prevent overshooting likely minima as we pass over batch
        val decayedAlpha = (a * (1 - trainSetPos / trainSetSize)).max(MIN_LEARNING_RATE)
        val contextSize = trainingSet.leafSet.size
        var contextPos = 0

        //loop through context
        while (contextPos < contextSize) {
          val leafIndex = trainingSet.leafSet(contextPos).toInt
          val leafVector = weights._1.getOrElse(leafIndex, Array.empty[Double])
          val hiddenVector = new Array[Double](vectorSize)
          var codePos = 0

          //loop through HSM tree inner nodes
          while (codePos < trainingSet.innerSet.codeDepth) {
            val innerIndex = trainingSet.innerSet.innerIndex(codePos).toInt
            val innerVector = weights._2.getOrElse(innerIndex, Array.empty[Double])

            var forwardPass =
              blas.ddot(vectorSize, leafVector, 0, 1,
                innerVector, 0, 1)

            //test if the forward pass would fall into precomputed sigmoid
            if (forwardPass > -MAX_EXP && forwardPass < MAX_EXP) {
              val expIndex = ((forwardPass + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt

              //reassign forwardPass to sigmoidal output of result
              forwardPass = expTable(expIndex)

              //error function between expected tree traversal - 1 or 0 -
              // and output probability of same
              val gradient =
                (1 - trainingSet.innerSet.code(codePos) - forwardPass) * decayedAlpha

              //backprop gradient from innerVector to hiddenVector
              blas.daxpy(vectorSize, gradient, innerVector, 0, 1,
                hiddenVector, 0, 1)

              //backprop gradient from leafVector to innerVector
              blas.daxpy(vectorSize, gradient, leafVector, 0, 1,
                innerVector, 0, 1)
              innerModify.update(innerIndex, 1)
            }
            codePos += 1
          }
          //backprop hiddenVector from path traversal onto leafVector
          blas.daxpy(vectorSize, 1.0f, hiddenVector, 0, 1,
            leafVector, 0, 1)
          leafModify.update(leafIndex, 1)
          contextPos += 1
        }
        (leafWeights, innerWeights, decayedAlpha, trainSetPos + 1, trainSetSize)
    }
    //returns sparse representations of updated vectors
    val sparseLeaf = model._1.filterKeys(x => leafModify(x.toInt) > 0).toSeq
    val sparseInner = model._2.filterKeys(x => leafModify(x.toInt) > 0).toSeq
    sparseLeaf -> sparseInner
  }

  /** convenience function to allows weights to be broadcast in function with HSMTrainingSet
    *
    * @param context
    * @param weights
    * @return
    */
  private def train(
  context: Seq[HSMTrainingSet],
  weights: HSMWeightMatrix[T])
  : (Seq[(Long, Array[Double])], Seq[(Long, Array[Double])]) =
    train(context, weights.leafVectors -> weights.innerVectors, None)

  /** generates digitized values for sigmoidal expression to speed up learning
    *
    * @return
    */
  private def createExpTable(): Array[Double] = {
    val expTable = new Array[Double](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = scala.math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = tmp / (tmp + 1.0)
      i += 1
    }
    expTable
  }
}
