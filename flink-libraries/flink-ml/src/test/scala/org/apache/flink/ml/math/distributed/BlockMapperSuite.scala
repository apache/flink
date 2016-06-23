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

package org.apache.flink.ml.math.distributed

import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class BlockMapperSuite
    extends FlatSpec
    with Matchers
    with GivenWhenThen
    with FlinkTestBase {

  val simpleMapper = BlockMapper(90, 60, 12, 7)

  "getBlockId" should "return a valid blockId" in {
    Given("valid block coordinates")

    val verySimpleMapper = BlockMapper(4, 4, 2, 2)

    simpleMapper.getBlockIdByCoordinates(0, 0) shouldBe 0
    simpleMapper.getBlockIdByCoordinates(37, 24) shouldBe 30
    simpleMapper.getBlockIdByCoordinates(89, 59) shouldBe simpleMapper.numBlocks -
    1
  }

  it should "throw an exception" in {
    Given("invalid coordinates")
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockIdByCoordinates(-1, 0)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockIdByCoordinates(0, -1)
    }

    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockIdByCoordinates(10000, 10000)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockIdByCoordinates(10000, 0)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockIdByCoordinates(0, 10000)
    }
  }

  "getBlockCoordinates" should "return valid coordinates" in {
    Given("a valid block id")

    simpleMapper.getBlockMappedCoordinates(3) shouldBe (0, 3)
    simpleMapper.getBlockMappedCoordinates(0) shouldBe (0, 0)
    simpleMapper.getBlockMappedCoordinates(9) shouldBe (1, 0)
    simpleMapper.getBlockMappedCoordinates(10) shouldBe (1, 1)
    simpleMapper.getBlockMappedCoordinates(15) shouldBe (1, 6)
    simpleMapper.getBlockMappedCoordinates(48) shouldBe (5, 3)
    simpleMapper.getBlockMappedCoordinates(70) shouldBe (7, 7)
  }
  it should "throw an exception" in {
    Given("a non valid blockId")
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockMappedCoordinates(-1)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      simpleMapper.getBlockMappedCoordinates(1000000)
    }
  }

  "BlockMapper constructor" should "create a valid BlockMapper" in {

    Given("valid parameters")
    val mapper = BlockMapper(200, 60, 64, 7)
    mapper.numBlockRows shouldBe 4
    mapper.numBlockCols shouldBe 9
  }
}
