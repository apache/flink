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

package org.apache.flink.runtime.client;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.utils.FakeTestingCluster;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobClientTest {

	private static ActorSystem system;

	protected Logger logger = LoggerFactory.getLogger(JobClientTest.class);

	@Before
	public void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@After
	public void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testJobManagerTimeout(){
		new JavaTestKit(system){{
			FakeTestingCluster cluster = null;
			try{
				cluster = new FakeTestingCluster("idle");
				try {
					JobClient.submitJobAndWait(
							new Configuration(),
							system,
							cluster.getJobManagerGateway(),
							new JobGraph("test graph"),
							AkkaUtils.INF_TIMEOUT(),
							false);
				} catch(JobExecutionException e){
					assertTrue(e.getMessage().contains("timed out"));
				}
			} catch(Exception e){
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				if(cluster != null){
					cluster.stopCluster();
				}
			}
		}};
	}

	@Test
	public void testJobStuckCreated(){
		new JavaTestKit(system){{
			FakeTestingCluster cluster = null;
			try{
				cluster = new FakeTestingCluster("busy");
				try {
					JobClient.submitJobAndWait(
							new Configuration(),
							system,
							cluster.getJobManagerGateway(),
							new JobGraph("test graph"),
							AkkaUtils.INF_TIMEOUT(),
							false);
				} catch(JobExecutionException e){
					assertTrue(e.getMessage().contains("Job hasn't been running"));
				}
			} catch(Exception e){
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				if(cluster != null){
					cluster.stopCluster();
				}
			}
		}};
	}

	@Test
	public void testJobStuckRestarting(){
		new JavaTestKit(system){{
			FakeTestingCluster cluster = null;
			try{
				cluster = new FakeTestingCluster("failed");
				try {
					JobClient.submitJobAndWait(
							new Configuration(),
							system,
							cluster.getJobManagerGateway(),
							new JobGraph("test graph"),
							AkkaUtils.INF_TIMEOUT(),
							false);
				} catch(JobExecutionException e){
					assertTrue(e.getMessage().contains("Job hasn't been running"));
				}
			} catch(Exception e){
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				if(cluster != null){
					cluster.stopCluster();
				}
			}
		}};
	}
}
