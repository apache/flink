package org.apache.flink.ml.preprocessing

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.apache.flink.ml.preprocessing.StringIndexer.HandleInvalid
import org.apache.flink.api.scala.utils._

import scala.collection.immutable.Seq

/**
  * A label indexer that maps a string label to an index.
  * The indices are in [0, numLabels), ordered by label frequencies.
  * The most frequent label gets index 0.
  */
class StringIndexer extends Transformer[StringIndexer] {

  private[preprocessing] var metricsOption: Option[DataSet[(String, Long)]] = None


  def setHandleInvalid(value: String): this.type ={
    parameters.add( HandleInvalid, value )
    this
  }

}

object StringIndexer {

  case object HandleInvalid extends Parameter[String] {
    val defaultValue: Option[String] = Some( "skip" )
  }

  // ==================================== Factory methods ==========================================

  def apply(): StringIndexer ={
    new StringIndexer( )
  }

  // ====================================== Operations =============================================

  /**
    * Trains [[StringIndexer]] by learning the count of each string in the input DataSet.
    */

  implicit def fitStringIndexer ={
    new FitOperation[StringIndexer, String] {
      def fit(instance: StringIndexer, fitParameters: ParameterMap, input: DataSet[String]):Unit ={
        val metrics = extractIndices( input )
        instance.metricsOption = Some( metrics )
      }
    }
  }

  /**
    * Sort the labels by frequency and assign an index.
    *
    * @param input a dataset containing labels
    * @return a new dataset with an index for each label
    */
  private def extractIndices(input: DataSet[String]): DataSet[(String, Long)] = {

    val mapping = input
      .mapWith( s => (s, 1) )
      .groupBy( 0 )
      .sum(1)
      .partitionByRange( x => - x._2 )
      .sortPartition(1, Order.DESCENDING)
      .zipWithIndex
      .mapWith { case (id, (label, count)) => (label, id) }

    mapping.print()

    mapping
  }

  /**
    * [[TransformDataSetOperation]] which returns a new dataset of (label,index)
    * If "skip" is choosen, unseen labels are ignored and the corresponding lines
    * are skipped. Otherwise we throw an Exception.
    */

  implicit def transformStringDataset ={
    new TransformDataSetOperation[StringIndexer, String, (String, Long)] {
      def transformDataSet(instance: StringIndexer,
                           transformParameters: ParameterMap,
                           input: DataSet[String]) ={

        val resultingParameters = instance.parameters ++ transformParameters
        val handleInvalid = resultingParameters( HandleInvalid )

        def toHandle(label: String) = handleInvalid match {
          case "skip"  => Seq.empty[(String,Long)]
          case _ => throw new Exception(s"label ${label} has not be fitted during the fit phase - " +
            s"Use setHandleInvalid with skip parameter to filter non fitted labels, or fit your data with " +
            s"label ${label}")
        }

        instance.metricsOption match {
          case Some(metrics) => {
            input
              .leftOuterJoin(metrics).where("*").equalTo(0) {
                (left,right) =>
                  val joinIndex = if(right == null) None else Some(right._2)
                  (left,joinIndex)
              }
            .flatMapWith{ case(label, index) =>
              index match {
                case Some(value) => Seq((label,value))
                case _ => toHandle(label)
              }
            }
          }

          case None =>
            throw new RuntimeException( "The StringIndexer has to be fitted to the data. " +
              "This is necessary to determine the count" )
        }
      }
    }
  }
}
