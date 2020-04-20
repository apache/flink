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

package org.apache.flink.api.scala.manual

import java.io._
import java.util.Random

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory
import org.apache.flink.api.scala._
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync
import org.apache.flink.runtime.memory.MemoryManagerBuilder
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger
import org.apache.flink.runtime.operators.testutils.DummyInvokable
import org.apache.flink.util.{MutableObjectIterator, TestLogger}
import org.junit.Assert._

/**
 * This test is wrote as manual test.
 */
class MassiveCaseClassSortingITCase extends TestLogger {
  
  val SEED : Long = 347569784659278346L
  
  def testStringTuplesSorting() {
    
    val NUM_STRINGS = 3000000
    var input: File = null
    var sorted: File = null
    
    try {
      input = generateFileWithStringTuples(NUM_STRINGS,
                                           "http://some-uri.com/that/is/a/common/prefix/to/all")
        
      sorted = File.createTempFile("sorted_strings", "txt")
      
      val command = Array("/bin/bash", "-c", "export LC_ALL=\"C\" && cat \""
                        + input.getAbsolutePath + "\" | sort > \"" + sorted.getAbsolutePath + "\"")

      var p: Process = null
      try {
        p = Runtime.getRuntime.exec(command)
        val retCode = p.waitFor()
        if (retCode != 0) {
          throw new Exception("Command failed with return code " + retCode)
        }
        p = null
      }
      finally {
        if (p != null) {
          p.destroy()
        }
      }
      
      var sorter: UnilateralSortMerger[StringTuple] = null
      
      var reader: BufferedReader = null
      var verifyReader: BufferedReader = null
      
      try {
        reader = new BufferedReader(new FileReader(input))
        val inputIterator = new StringTupleReader(reader)
        
        val typeInfo = implicitly[TypeInformation[StringTuple]]
          .asInstanceOf[CompositeType[StringTuple]]
        
        val serializer = typeInfo.createSerializer(new ExecutionConfig)
        val comparator = typeInfo.createComparator(
          Array(0, 1),
          Array(true, true),
          0,
          new ExecutionConfig)
        
        val mm = MemoryManagerBuilder.newBuilder.setMemorySize(1024 * 1024).build
        val ioMan = new IOManagerAsync()
        
        sorter = new UnilateralSortMerger[StringTuple](mm, ioMan, inputIterator,
              new DummyInvokable(),
              new RuntimeSerializerFactory[StringTuple](serializer, classOf[StringTuple]),
              comparator, 1.0, 4, 0.8f, true /*use large record handler*/, false)
            
        val sortedData = sorter.getIterator
        reader.close()
        
        verifyReader = new BufferedReader(new FileReader(sorted))
        val verifyIterator = new StringTupleReader(verifyReader)
        
        var num = 0
        var hasMore = true
        
        while (hasMore) {
          val next = verifyIterator.next(null)
          
          if (next != null ) {
            num += 1
            
            val nextFromFlinkSort = sortedData.next(null)
            
            assertNotNull(nextFromFlinkSort)
            
            assertEquals(next.key1, nextFromFlinkSort.key1)
            assertEquals(next.key2, nextFromFlinkSort.key2)
            
            // assert array equals does not work here
            assertEquals(next.value.length, nextFromFlinkSort.value.length)
            for (i <- 0 until next.value.length) {
              assertEquals(next.value(i), nextFromFlinkSort.value(i))
            }
            
          }
          else {
            hasMore = false
          }
        }
        
        assertNull(sortedData.next(null))
        assertEquals(NUM_STRINGS, num)
      }
      finally {
        if (reader != null) {
          reader.close()
        }
        if (verifyReader != null) {
          verifyReader.close()
        }
        if (sorter != null) {
          sorter.close()
        }
      }
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        e.getMessage
      }
    }
    finally {
      if (input != null) {
        input.delete()
      }
      if (sorted != null) {
        sorted.delete()
      }
    }
  }
  
  
  private def generateFileWithStringTuples(numStrings: Int, prefix: String): File = {
    val rnd = new Random(SEED)
    val bld = new StringBuilder()
    val f = File.createTempFile("strings", "txt")
    
    var wrt: BufferedWriter = null
    
    try {
      wrt = new BufferedWriter(new FileWriter(f))

      for (i <- 0 until numStrings) {
        bld.setLength(0)
        val numComps = rnd.nextInt(5) + 2
        
        for (z <- 0 until numComps) {
          if (z > 0) {
            bld.append(' ')
          }
          bld.append(prefix)
          val len = rnd.nextInt(20) + 10
          
          for (k <- 0 until len) {
            val c = (rnd.nextInt(80) + 40).toChar
            bld.append(c)
          }
        }
        val str = bld.toString
        wrt.write(str)
        wrt.newLine()
      }
    }
    finally {
      wrt.close()
    }
    f
  }
}

object MassiveCaseClassSortingITCase {
  
  def main(args: Array[String]) {
    new MassiveCaseClassSortingITCase().testStringTuplesSorting()
  }
}

case class StringTuple(key1: String, key2: String, value: Array[String])
  
class StringTupleReader(val reader: BufferedReader) extends MutableObjectIterator[StringTuple] {
  
  override def next(reuse: StringTuple): StringTuple = {
    val line = reader.readLine()
    if (line == null) {
      return null
    }
    val parts = line.split(" ")
    StringTuple(parts(0), parts(1), parts)
  }

  override def next(): StringTuple = {
    val line = reader.readLine()
    if (line == null) {
      return null
    }
    val parts = line.split(" ")
    StringTuple(parts(0), parts(1), parts)
  }

}
