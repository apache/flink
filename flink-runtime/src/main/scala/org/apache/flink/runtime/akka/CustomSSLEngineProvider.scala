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


import akka.remote.transport.netty.ConfigSSLEngineProvider
import javax.net.ssl.{TrustManager, TrustManagerFactory}
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.FingerprintTrustManagerFactory

class CustomSSLEngineProvider(system : akka.actor.ActorSystem)
                              extends ConfigSSLEngineProvider(system) {

  private val securityConfig = system.settings.config.getConfig("akka.remote.netty.ssl.security")
  private val SSLTrustStore = securityConfig.getString("trust-store")
  private val SSLTrustStorePassword = securityConfig.getString("trust-store-password")
  private val SSLCertFingerprints = securityConfig.getStringList("cert-fingerprints")

  override protected def trustManagers: Array[TrustManager] = {

    val trustManagerFactory = if (SSLCertFingerprints.isEmpty) {
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    } else new FingerprintTrustManagerFactory(SSLCertFingerprints)

    trustManagerFactory.init(loadKeystore(SSLTrustStore, SSLTrustStorePassword))
    trustManagerFactory.getTrustManagers
  }
}
