package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.apache.flink.ml.preprocessing.StringIndexer.HandleInvalid

import scala.collection.immutable.Seq

/**
  * String Indexer
  */
class StringIndexer extends Transformer[StringIndexer] {

  private[preprocessing] var metricsOption: Option[Map[String, Int]] = None


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

  private def extractIndices(input: DataSet[String]): Map[String, Int] = {

    implicit val resultTypeInformation = createTypeInformation[(String, Int)]

    val mapper = input
      .map( s => (s, 1) )
      .groupBy( 0 )
      .reduce( (a, b) => (a._1, a._2 + b._2) )
      .collect( )
      .sortBy( r => (r._2, r._1) )
      .zipWithIndex
      .map { case ((s, c), ind) => (s, ind) }
      .toMap

    mapper
  }

  /**
    * [[TransformDataSetOperation]] which returns a new dataset with the index added
    */

  implicit def transformStringDataset ={
    new TransformDataSetOperation[StringIndexer, String, (String, Int)] {
      def transformDataSet(instance: StringIndexer, transformParameters: ParameterMap, input: DataSet[String]) ={

        val resultingParameters = instance.parameters ++ transformParameters
        val handleInvalid = resultingParameters( HandleInvalid )

        def toHandle(label: String) = handleInvalid match {
          case "skip"  => Seq.empty[(String,Int)]
          case _ => throw new Exception(s"label ${label} has not be fitted during the fit phase - " +
            s"Use setHandleInvalid with skip parameter to filter non fitted labels, or fit your data with " +
            s"label ${label}")
        }

        instance.metricsOption match {
          case Some( metrics ) => {
            input.flatMap { l =>
              val count = metrics.get(l)
              count match {
                case Some(value) => Seq((l,value))
                case None => Seq.empty[(String,Int)] //toHandle(l)
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
