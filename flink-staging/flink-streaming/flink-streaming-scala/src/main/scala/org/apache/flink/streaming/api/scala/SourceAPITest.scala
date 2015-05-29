package org.apache.flink.streaming.api.scala


import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.io.{TextInputFormat, CsvInputFormat}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType

object SourceAPITest {

	def main(args: Array[String]): Unit = {
		val javaEnv: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironment()
		val env: StreamExecutionEnvironment = new StreamExecutionEnvironment(javaEnv)
		val sequence = List(2,3,4,5,6)
		val iter = sequence.iterator
		env.socketTextStream("localhost",9998, ' ', 1L).print()
		env.execute()
	}

}
