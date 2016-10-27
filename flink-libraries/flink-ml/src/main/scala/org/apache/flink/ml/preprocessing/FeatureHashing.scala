package org.apache.flink.ml.preprocessing

import breeze.linalg
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.pipeline.{TransformDataSetOperation, FitOperation, Transformer}
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.preprocessing.FeatureHashing.{HashingFunction, Binary}

import HashingTF._

import scala.collection.mutable

/**
  * Feature hashes a DataSet using algorithm in Spark
  */
class FeatureHashing extends Transformer[FeatureHashing] {

  private[preprocessing] var hashingOption: Option[DataSet[linalg.Vector[Double]]] = None


  def setHashingFunction(value: String): this.type ={
    parameters.add( HashingFunction, value.toLowerCase )
    this
  }

  def setBinary(toBinary: Boolean): this.type ={
    parameters.add( Binary, toBinary )
    this
  }

}

object FeatureHashing {

  // ====================================== Parameters =============================================

  case object Binary extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = Some( false )
  }

  case object HashingFunction extends Parameter[String] {
    val defaultValue: Option[String] = Some( "murmur3" )
  }

  // ==================================== Factory methods ==========================================
  def apply(): FeatureHashing ={
    new FeatureHashing( )
  }

  // ====================================== Operations =============================================

  implicit def fitFeatureHashing[T] ={
    new FitOperation[FeatureHashing, T] {
      def fit(instance: FeatureHashing, fitParameters: ParameterMap, input: DataSet[T]): Unit ={
        /* DO NOTHING */
        println( "Fit operation do nothing yet" )
      }
    }
  }


  implicit def transformIterable[T](data: DataSet[T]) ={
    new TransformDataSetOperation[FeatureHashing, T, Vector] {
      def transformDataSet(instance: FeatureHashing, transformParameters: ParameterMap, input: DataSet[T]) ={

        val termFrequencies = mutable.HashMap.empty[Int, Double]

        val resultingParameters = instance.parameters ++ transformParameters

        val binary = resultingParameters( Binary ) match {
          case Some( _binary ) => true
          case None => false
        }
        val setTF = if (binary) (i: Int) => 1.0 else (i: Int) => termFrequencies.getOrElse( i, 0.0 ) + 1.0

        val hashingFunction = resultingParameters( HashingFunction ) match {
          case Some( name ) => getHashFunction( name.asInstanceOf[String] )
          case None => nativeHash
        }

        input.map {
          document =>
            document match {
              case d: Iterable[_] =>
                val vect = d.map( e => setTF( hashingFunction( e ) ) )
              // Then produce vector
              case e: Any => setTF( hashingFunction( e ) )
            }
        }
        ???
      }
    }
  }


  private def getHashFunction(name: String): Any => Int = name match {
    case "native" => nativeHash
    case "murmur3" => murmur3Hash
    case _ => throw new Exception( s"The hash function ${name} is not implemented - native and murmur3 are the hash functions currently implemented" )
  }

}

object HashingTF {

  val nativeHash = (term: Any) => term.##

  val murmur3Hash = (term: Any) => {
    3
  }

}
