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

package org.apache.flink.runtime.akka

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.runtime.jobmanager.JobManager
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfterAll, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AkkaUtilsTest
  extends FunSuite
  with Matchers
  with BeforeAndAfterAll {

  test("getHostFromAkkaURL should return the correct host from a remote Akka URL") {
    val host = "127.0.0.1"
    val port = 1234

    val address = new InetSocketAddress(host, port)

    val remoteAkkaURL = JobManager.getRemoteJobManagerAkkaURL(
      address,
      Some("actor"))

    val result = AkkaUtils.getInetSockeAddressFromAkkaURL(remoteAkkaURL)

    result should equal(address)
  }

  test("getHostFromAkkaURL should throw an exception if the InetSocketAddress cannot be " +
    "retrieved") {
    val localAkkaURL = JobManager.getLocalJobManagerAkkaURL(Some("actor"))

    intercept[Exception] {
      AkkaUtils.getInetSockeAddressFromAkkaURL(localAkkaURL)
    }
  }

  test("getHostFromAkkaURL should return host after at sign") {
    val url = "akka://flink@localhost:1234/user/jobmanager"
    val expected = new InetSocketAddress("localhost", 1234)

    val result = AkkaUtils.getInetSockeAddressFromAkkaURL(url)

    result should equal(expected)
  }

}
