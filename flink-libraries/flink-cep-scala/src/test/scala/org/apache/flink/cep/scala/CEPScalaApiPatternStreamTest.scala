package org.apache.flink.cep.scala

import java.lang.reflect.Field

import org.apache.flink.cep
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert.assertEquals
import org.junit.Test

class CEPScalaApiPatternStreamTest {
  /**
    * These tests simply check that use the Scala API  to update the TimeCharacteristic of the PatternStream .
    */

  @Test
  def updateCepTimeCharacteristicByScalaApi(): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStreamSource[Event] = env.fromElements(Event(1, "barfoo", 1.0), Event(8, "end", 1.0))
    val pattern: Pattern[Event, Event] = Pattern.begin("start").where(new SimpleCondition[Event]() {
      override def filter(value: Event): Boolean = value.name == "start"
    })

    val jestream: cep.PatternStream[Event] = org.apache.flink.cep.CEP.pattern(input, pattern)

    //get org.apache.flink.cep.scala.PatternStream
    val sePstream = new PatternStream[Event](jestream)

    //get  TimeBehaviour
    val time1: AnyRef = getTimeBehaviourFromScalaPatternStream(sePstream)

    assertEquals(time1.toString, "EventTime")

    //change TimeCharacteristic use scala api
    val sPstream: PatternStream[Event] = sePstream.inProcessingTime()

    //get  TimeBehaviour
    val time2: AnyRef = getTimeBehaviourFromScalaPatternStream(sPstream)

    assertEquals(time2.toString, "ProcessingTime")


  }

  def getTimeBehaviourFromScalaPatternStream(seStream: org.apache.flink.cep.scala.PatternStream[Event])  = {
    val field: Field = seStream.getClass.getDeclaredField("jPatternStream")
    field.setAccessible(true)
    val JPattern: AnyRef = field.get(seStream)
    val stream: cep.PatternStream[Event] = JPattern.asInstanceOf[cep.PatternStream[Event]]
    getTimeBehaviourFromJavaPatternStream(stream)
  }

  def getTimeBehaviourFromJavaPatternStream(jeStream: org.apache.flink.cep.PatternStream[Event])={
    val builder: Field = jeStream.getClass.getDeclaredField("builder")
    builder.setAccessible(true)
    val o: AnyRef = builder.get(jeStream)
    val timeBehaviour: Field = o.getClass.getDeclaredField("timeBehaviour")
    timeBehaviour.setAccessible(true)
    timeBehaviour.get(o)
  }


  case class  Event(id:Int ,name:String ,price:Double)
}
