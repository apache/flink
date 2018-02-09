package org.apache.flink.streaming.api.scala

import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

/**
  * Integration test for [[DataStreamUtils.reinterpretAsKeyedStream()]].
  */
class ReinterpretDataStreamAsKeyedStreamITCase {

  @Test
  def testReinterpretAsKeyedStream(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.fromElements(1, 2, 3)
    new DataStreamUtils(source).reinterpretAsKeyedStream((in) => in)
      .timeWindow(Time.seconds(1))
      .reduce((a, b) => a + b)
      .addSink(new DiscardingSink[Int])
    env.execute()
  }
}
