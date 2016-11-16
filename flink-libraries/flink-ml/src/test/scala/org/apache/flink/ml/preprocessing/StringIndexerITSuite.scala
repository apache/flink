package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}


class StringIndexerITSuite  extends FlatSpec
with Matchers
with FlinkTestBase {

  behavior of "Flink's StringIndexer"

  it should "fit and transform a DataSet[String] to a DataSet[(String,Long)] with StringIndexer" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val fitData = env.fromCollection(List("a","b","c","a","a","d","a","a","a","b","b","c","a","c","b","c"))
    val rawData = env.fromCollection(List("a","a","b","b","e","d","a","a"))

    val stringIndexer = StringIndexer()
      .setHandleInvalid("skip")

    stringIndexer.fit(fitData)

    val transformedDataFromFitted = stringIndexer.transform(fitData).collect().toList
    val transformedDataFromRawData = stringIndexer.transform(rawData).collect().toList

    transformedDataFromFitted.size should be (16)
    transformedDataFromRawData.size should be (7)

    for( (label,count) <- (transformedDataFromFitted ::: transformedDataFromRawData)) {

      if (label == "a") count should be (0)
      else if (label == "b") count should be (1)
      else if(label == "c") count should be (2)
      else count should be (3)
    }
  }

}
