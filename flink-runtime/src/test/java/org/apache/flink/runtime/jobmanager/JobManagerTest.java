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
package org.apache.flink.runtime.jobmanager;

import com.typesafe.config.Config;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.junit.Test;
import scala.Some;
import scala.Tuple2;

import java.net.InetAddress;

import static org.junit.Assert.*;

public class JobManagerTest {

	@Test
	public void testNullHostnameGoesToLocalhost() {
		try {
			Tuple2<String, Object> address = new Tuple2<String, Object>(null, 1772);
			Config cfg = AkkaUtils.getAkkaConfig(new Configuration(),
					new Some<Tuple2<String, Object>>(address));

			String hostname = cfg.getString("akka.remote.netty.tcp.hostname");
			assertTrue(InetAddress.getByName(hostname).isLoopbackAddress());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
