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

package org.apache.flink.ml.clustering

import breeze.linalg._
import breeze.numerics._
import org.apache.flink.api.scala._
import org.apache.flink.ml.common._

case class Gaussian(id: Long, prior: Double, mu: DenseVector[Double], sigma: DenseMatrix[Double]) {}
case class Point(id: Long, pos: DenseVector[Double]) {}

case class Liklihood(pointID: Long, gaussianID: Long, probability: Double) {}
case class TotalLiklihood(pointID:Long,probTotal:Double){}
case class Prediction(gaussianID: Long, pointID: Long, probability: Double) {}
/**
 * Implements the Gaussian Mixture Model
 * Which is a sound way of soft clustering grounded-probabilistically, where each cluster is a generative model(Gaussian)
 * The Expectation Maximization algorithms is used to discover parameters(priors,means,covariances)
 * for "K" sources(Gaussians)
 *
 * EM Algorithm:
 * Starts with provided or randomly placed Gaussians
 * Foreach data-point what is the probability that it comes from Gaussian g based on
 * the liklihood which is calculate by gaussian distribution function
 * Adjust the parameters each Gaussian to fit data points assigned to them.
 * Repeat the algorithm to the number of required number of iterations
 *
 * There are two steps in the algorithm
 *
 * ========== Expectation step ==========
 * calculates the posterior for each data point using data points and initial Gaussians.
 * as an example computes the posterior P(c|x) for a given sample 'x' according to gaussian 'c'. ---
 *
 *
 * ========== Maximization step ==========
 * Maximizes the parameters(Priors, Means and  Covariance or Sigma).
 *
 * Further details of the algorithm can be found [[[http://en.wikipedia.org/wiki/Expectation-maximization_algorithm here]].
 *
 * [[org.apache.flink.ml.clustering.GMM]] is a [[org.apache.flink.ml.common.Learner]] which
 * needs to be trained on a set of data points and emits a [[org.apache.flink.ml.clustering.GMModel]]
 * which is a [[org.apache.flink.ml.common.Transformer]] to calculate the responsibility of data points
 * for particular Gaussian or cluster.
 *
 *   * @example
 *          {{{
 *             val trainingDS:DataSet[Point(id:Int,DenseVector[Double])] = env.fromCollection(Seq[Point])
 *             val testingDS:DataSet[Point(id:Int,DenseVector[Double])] = env.fromCollection(Seq[Point])
 *             val gaussiansDS:DataSet[Gaussians(id:Long,prior:Double,mu:DenseVector[Double],sigma: DenseMatrix[Double])]=env.fromCollection(Seq[Gaussian])
 *
 *             val learner = GMM()
 *             .setInitialGaussians(gaussiansDS)
 *             .setNumIterations(30)
 *
 *             val gModel = learner.fit(trainingDS)
 *             val resultedPredictionsDS = gModel.transform(testingDS)
 *             val resultedGaussiansDS = gModel.gmm
 *          }}}
 *
 * =Parameters=
 *
 * -[[org.apache.flink.ml.clustering.GMM..NumIterations]]
 * Defines the maximum number of iterations for the algorithm to repeat the EM algorithm.
 * (Default value: '''10''')
 *
 * -[[org.apache.flink.ml.clustering.GMM..NumGaussians]]
 * Defines the maximum number of clusters or gaussians to be trained by the training data set.
 * It will be provided only if the gaussians are to be placed randomly
 * (Default value: '''2''')
 *
 * -[[org.apache.flink.ml.clustering.GMM.InitialGaussians]]
 * Sets the initial gaussians provided by the user pre initialized with prior, means and sigma.
 * (Default value: '''None''') in which case this class will create the number of gaussians according
 * to the [[org.apache.flink.ml.clustering.GMM..NumGaussians]] initialized each with
 * prior, random mu and sigma adjusted according to the mu.
 *
 */
class GMM extends Learner[Point,GMModel] {

  import GMM._

  /** Sets the number of iterations the EM algorithm will repeate
    *
    * @param numIterations
    * @return itself
    */
  def setNumIterations(numIterations: Int): GMM = {
    parameters.add(NumIterations, numIterations)
    this
  }

  /** Sets the number of Clusters(Gaussians) in case to initialize the gaussians parameters randomly
    *
    * @param numGaussians
    * @return itself
    */
  def setNumGaussians(numGaussians: Int): GMM = {
    parameters.add(NumGaussians, numGaussians)
    this
  }

  /** Sets the initial gaussians with preinitialized parameters.
    *
    * @param initialGaussians
    * @return itself
    */
  def setInitialGaussians(initialGaussians:DataSet[Gaussian]):GMM ={
    parameters.add(InitialGaussians,initialGaussians)
    this
  }

  /** Trains a GMModel with soft-clustering based on the given training data set.
    *
    * @param input Training data set
    * @param fitParameters Parameter values
    * @return Trained GMM model
    */
  override def fit(input: DataSet[Point], fitParameters: ParameterMap): GMModel = {

    val resultingParameters = this.parameters ++ fitParameters

    val numIterations: Int = resultingParameters.get(NumIterations).get
    val numPoints:Int=input.count().toInt
    val numDimensions:Int=input.collect().apply(0).pos.length
    val initialGaussians:DataSet[Gaussian]=resultingParameters.get(InitialGaussians).get

    // generate random gaussians if not provided by the user
    if(initialGaussians==None){
      val numGaussians:Int=resultingParameters.get(NumGaussians).get
      val gaussiansArray =(
          for (i <- 1 to numGaussians) yield Gaussian(
            i,
            1 / numGaussians,
            DenseVector.rand(numDimensions),
            DenseMatrix.rand(numDimensions, numDimensions))).toArray
      val initialGaussians= ExecutionEnvironment.getExecutionEnvironment.fromElements(gaussiansArray:_*)
    }

    //Adjust the sigmas according to means for achieve more accuricy
    val normGaussiansCovariance=(initialGaussians cross input).map(pair=>
    {
      val gaussian=pair._1  //gaussian
      val point=pair._2     //point
      Gaussian(gaussian.id,
                gaussian.prior,
                gaussian.mu,
                (point.pos-gaussian.mu) * new Transpose(point.pos-gaussian.mu))}) //sigma
        .groupBy(x=>x.id).reduce((g1,g2)=>{
               Gaussian(g1.id,g1.prior,g1.mu,g1.sigma+g2.sigma)})
        .map(g=>Gaussian(g.id,g.prior,g.mu, g.sigma / (numPoints-1.0)))

    //Repeat the EM algorithm based on number of iterations
    val finalGaussians = normGaussiansCovariance.iterate(numIterations) { initialGaussians =>

      //E-Step
      //estimate the probability of sample 'pair' under the distribution of component 'gaussian'
      val pointsProb = (initialGaussians cross input)
        .map(pair => {
                      val c = pair._1 // gaussian
                      val x = pair._2 // point
                      val invSigma = inv(c.sigma)

                      //density function for gaussian distribution
                      val const = 1 / (sqrt(2 * Math.PI) * abs(det(c.sigma)))
                      val liklehood = const * exp(-0.5 * (new Transpose(x.pos-c.mu) * invSigma * (x.pos-c.mu))) * c.prior

        Liklihood(x.id, c.id, liklehood)})

      //total probability for sample 'x' generated by all 'gaussians'
      val liklihoodTotal = pointsProb
        .map(p => TotalLiklihood(p.pointID, p.probability))
        .groupBy(_.pointID)
        .reduce((p1, p2) => TotalLiklihood(p1.pointID, p1.probTotal + p2.probTotal))

      //compute the posteriori P(c|x) for sample 'x' to belong to gaussian 'c'
      val posterior = pointsProb
        .joinWithTiny(liklihoodTotal)
        .where(l => l.pointID)
        .equalTo(r => r.pointID)
        .apply((p1, p2) => {
        Prediction(p1.gaussianID, p1.pointID, p1.probability / p2.probTotal)
      })

      //M-Step
      //maximize the parameters
      val weightedGaussians = posterior.joinWithTiny(input).where(l => l.pointID).equalTo(r => r.id)
        .map(pair => {
                        val post = pair._1  //posterior
                        val point = pair._2 //point
                        Gaussian(post.gaussianID,
                                  post.probability,
                                  post.probability * point.pos,
                                  post.probability * (point.pos * new Transpose(point.pos)))})
        .groupBy(x => x.id)
        .reduce((g1, g2) => Gaussian(g1.id, g1.prior + g2.prior, g1.mu + g2.mu, g1.sigma + g2.sigma))

      val sumPriors = weightedGaussians
         .map(g => g.prior).reduce((p1, p2) => p1 + p2)

      val newGaussians = (weightedGaussians cross sumPriors)
        .map(pair => {
                        val gaussian = pair._1 //gaussian
                        val sumPr = pair._2    //sum of priors
                        val gProiorTot = gaussian.prior
                        val mu = gaussian.mu / gProiorTot
                        val sigma = (gaussian.sigma / gProiorTot) - (mu * new Transpose(mu))
                        val prior = gProiorTot / sumPr
                        Gaussian(gaussian.id, prior, mu, sigma)})

          newGaussians
    }

    GMModel(finalGaussians)
  }
}
/** Companion object of GMM. Contains convenience functions and the parameter type definitions
  * of the algorithm.
  */

object GMM {
  case object NumIterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }
  case object NumGaussians extends Parameter[Int]{
    val defaultValue=Some(2)
  }
  case object InitialGaussians extends Parameter[DataSet[Gaussian]]{
    val defaultValue=None
  }

  def apply():GMM={
    new GMM()
  }
}

/** Resulting GMM model calculated by the EM algorithm.
  *
  * @param gmm Calculated gaussians which will be used to assign data points to them based on their parameters using
  *            the density function for gaussian distribution and by calculating the posterior.
  */
case class GMModel(gmm: DataSet[Gaussian]) extends Transformer[Point, Prediction]
with Serializable {

  /** Calculates the prediction value of the SVM value (not the label)
    *
    * @param input [[DataSet]] containing the Point for which to calculate the predictions
    * @param parameters Parameter values for the algorithm
    * @return [[DataSet]] containing the Prediction of points for gaussians
    */
  override def transform(input: DataSet[Point], parameters: ParameterMap):
  DataSet[Prediction] = {
    val pointsProb = (gmm cross input).map(pair => {
      val c = pair._1 // gaussian
      val x = pair._2 // point
      val invSigma = inv(c.sigma)

      //density function for gaussian distribution
      val const = 1 / (sqrt(2 * Math.PI) * abs(det(c.sigma)))
      val liklihood = const * exp(-0.5 * (new Transpose(x.pos-c.mu) * invSigma * (x.pos-c.mu))) * c.prior

      Liklihood(x.id, c.id, liklihood)
    })

    //total probability for sample 'x' generated by all 'gaussians'
    val liklihoodTotal = pointsProb
      .map(p => TotalLiklihood(p.pointID, p.probability))
      .groupBy(_.pointID)
      .reduce((p1, p2) => TotalLiklihood(p1.pointID, p1.probTotal + p2.probTotal))

    //Prediction for sample 'x' belong to gaussian 'c'
    val prediction = pointsProb
      .joinWithTiny(liklihoodTotal)
      .where(l => l.pointID)
      .equalTo(r => r.pointID)
      .apply((p1, p2) => Prediction(p1.gaussianID, p1.pointID, p1.probability / p2.probTotal))

    prediction
  }

}