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
import java.util.Collections

import org.apache.flink.configuration.{AkkaOptions, Configuration, IllegalConfigurationException, SecurityOptions}
import org.apache.flink.runtime.clusterframework.BootstrapTools.FixedThreadPoolExecutorConfiguration
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.AddressResolution
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils.AkkaProtocol
import org.apache.flink.util.NetUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class AkkaUtilsTest
  extends FunSuite
  with Matchers
  with BeforeAndAfterAll {

  test("getAkkaConfig should validate transport heartbeats") {
    val configuration = new Configuration()
    configuration.setString(
      AkkaOptions.TRANSPORT_HEARTBEAT_PAUSE.key(),
      AkkaOptions.TRANSPORT_HEARTBEAT_INTERVAL.defaultValue())
    intercept[IllegalConfigurationException] {
      AkkaUtils.getAkkaConfig(configuration, Some(("localhost", 31337)))
    }
  }

  test("getHostFromAkkaURL should return the correct host from a remote Akka URL") {
    val host = "127.0.0.1"
    val port = 1234

    val address = new InetSocketAddress(host, port)

    val remoteAkkaUrl = AkkaRpcServiceUtils.getRpcUrl(
      host,
      port,
      "actor",
      AddressResolution.NO_ADDRESS_RESOLUTION,
      AkkaProtocol.TCP)

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(remoteAkkaUrl)

    result should equal(address)
  }

  test("getHostFromAkkaURL should throw an exception if the InetSocketAddress cannot be " +
    "retrieved") {
    val localAkkaURL = AkkaUtils.getLocalAkkaURL("actor")

    intercept[Exception] {
      AkkaUtils.getInetSocketAddressFromAkkaURL(localAkkaURL)
    }
  }

  test("getHostFromAkkaURL should return host after at sign") {
    val url = "akka://flink@localhost:1234/user/jobmanager"
    val expected = new InetSocketAddress("localhost", 1234)

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(url)

    result should equal(expected)
  }

  test("getHostFromAkkaURL should handle 'akka.tcp' as protocol") {
    val url = "akka.tcp://flink@localhost:1234/user/jobmanager"
    val expected = new InetSocketAddress("localhost", 1234)

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(url)

    result should equal(expected)
  }

  test("getHostFromAkkaURL should handle 'akka.ssl.tcp' as protocol") {
    val url = "akka.ssl.tcp://flink@localhost:1234/user/jobmanager"
    val expected = new InetSocketAddress("localhost", 1234)

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(url)

    result should equal(expected)
  }

  test("getHostFromAkkaURL should properly handle IPv4 addresses in URLs") {
    val IPv4AddressString = "192.168.0.1"
    val port = 1234
    val address = new InetSocketAddress(IPv4AddressString, port)

    val url = s"akka://flink@$IPv4AddressString:$port/user/jobmanager"

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(url)

    result should equal(address)
  }

  test("getHostFromAkkaURL should properly handle IPv6 addresses in URLs") {
    val IPv6AddressString = "2001:db8:10:11:12:ff00:42:8329"
    val port = 1234
    val address = new InetSocketAddress(IPv6AddressString, port)

    val url = s"akka://flink@[$IPv6AddressString]:$port/user/jobmanager"

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(url)

    result should equal(address)
  }

  test("getHostFromAkkaURL should properly handle IPv6 addresses in 'akka.tcp' URLs") {
    val IPv6AddressString = "2001:db8:10:11:12:ff00:42:8329"
    val port = 1234
    val address = new InetSocketAddress(IPv6AddressString, port)

    val url = s"akka.tcp://flink@[$IPv6AddressString]:$port/user/jobmanager"

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(url)

    result should equal(address)
  }

  test("getHostFromAkkaURL should properly handle IPv6 addresses in 'akka.ssl.tcp' URLs") {
    val IPv6AddressString = "2001:db8:10:11:12:ff00:42:8329"
    val port = 1234
    val address = new InetSocketAddress(IPv6AddressString, port)

    val url = s"akka.ssl.tcp://flink@[$IPv6AddressString]:$port/user/jobmanager"

    val result = AkkaUtils.getInetSocketAddressFromAkkaURL(url)

    result should equal(address)
  }

  test("getAkkaConfig should normalize the hostname") {
    val configuration = new Configuration()
    val hostname = "AbC123foOBaR"
    val port = 1234

    val akkaConfig = AkkaUtils.getAkkaConfig(configuration, hostname, port)

    akkaConfig.getString("akka.remote.netty.tcp.hostname") should
      equal(NetUtils.unresolvedHostToNormalizedString(hostname))
  }

  test("null hostname should go to localhost") {
    val configure = AkkaUtils.getAkkaConfig(new Configuration(), Some((null, 1772)))

    val hostname = configure.getString("akka.remote.netty.tcp.hostname")

    InetAddress.getByName(hostname).isLoopbackAddress should be(true)
  }

  test("getAkkaConfig defaults to fork-join-executor") {
    val akkaConfig = AkkaUtils.getAkkaConfig(new Configuration())

    akkaConfig.getString("akka.actor.default-dispatcher.executor") should
      equal("fork-join-executor")
  }

  test("getAkkaConfig sets executor with thread priority") {
    val threadPriority = 3
    val minThreads = 1
    val maxThreads = 3
    val akkaConfig = AkkaUtils.getAkkaConfig(
      new Configuration(),
      "localhost",
      1234,
      AkkaUtils.getThreadPoolExecutorConfig(
        new FixedThreadPoolExecutorConfiguration(minThreads, maxThreads, threadPriority)
      ))

    akkaConfig.getString("akka.actor.default-dispatcher.executor") should
      equal("thread-pool-executor")

    akkaConfig.getInt("akka.actor.default-dispatcher.thread-priority") should
      equal(threadPriority)
    akkaConfig.getInt("akka.actor.default-dispatcher.thread-pool-executor.core-pool-size-min")
      .should(equal(minThreads))
    akkaConfig.getInt("akka.actor.default-dispatcher.thread-pool-executor.core-pool-size-max")
      .should(equal(maxThreads))
  }

  test("getAkkaConfig should work with ipv6 addresses") {
    val ipv6AddressString = "2001:db8:10:11:12:ff00:42:8329"
    val configuration = new Configuration()
    val port = 1234

    val akkaConfig = AkkaUtils.getAkkaConfig(configuration, ipv6AddressString, port)

    akkaConfig.getString("akka.remote.netty.tcp.hostname") should
      equal(NetUtils.unresolvedHostToNormalizedString(ipv6AddressString))
  }

  test("getAkkaConfig should set startup timeout to be 10 times of ask timeout by default") {
    val configuration = new Configuration()
    configuration.setString(AkkaOptions.ASK_TIMEOUT.key(), "100ms")

    val akkaConfig = AkkaUtils.getAkkaConfig(configuration, Some(("localhost", 31337)))

    akkaConfig.getString("akka.remote.startup-timeout") should
      equal("1000ms")
  }

  test("getAkkaConfig for ssl engine provider without cert fingerprint") {
    val configuration = new Configuration()
    configuration.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true)

    val akkaConfig = AkkaUtils.getAkkaConfig(configuration, Some(("localhost", 31337)))

    val sslConfig = akkaConfig.getConfig("akka.remote.netty.ssl")

    sslConfig.getString("ssl-engine-provider") should
      equal("org.apache.flink.runtime.akka.CustomSSLEngineProvider")

    sslConfig.getStringList("security.cert-fingerprints") shouldBe empty
  }

  test("getAkkaConfig for ssl engine provider with cert fingerprint") {
    val configuration = new Configuration()
    configuration.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true)

    val fingerprint = "A8:98:5D:3A:65:E5:E5:C4:B2:D7:D6:6D:40:C6:DD:2F:B1:9C:54:36"
    configuration.setString(SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT, fingerprint)

    val akkaConfig = AkkaUtils.getAkkaConfig(configuration, Some(("localhost", 31337)))

    val sslConfig = akkaConfig.getConfig("akka.remote.netty.ssl")

    sslConfig.getString("ssl-engine-provider") should
      equal("org.apache.flink.runtime.akka.CustomSSLEngineProvider")

    sslConfig.getStringList("security.cert-fingerprints") should
      equal(Collections.singletonList(fingerprint))
  }
}
