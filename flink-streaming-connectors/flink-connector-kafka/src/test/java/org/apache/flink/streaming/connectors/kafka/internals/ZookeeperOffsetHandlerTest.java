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

package org.apache.flink.streaming.connectors.kafka.internals;

import kafka.admin.AdminUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;

import org.apache.flink.streaming.connectors.kafka.internals.ZookeeperOffsetHandler;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ZookeeperOffsetHandlerTest extends KafkaTestBase {
	
	@Test
	public void runOffsetManipulationinZooKeeperTest() {
		try {
			final String topicName = "ZookeeperOffsetHandlerTest-Topic";
			final String groupId = "ZookeeperOffsetHandlerTest-Group";
			
			final long offset = (long) (Math.random() * Long.MAX_VALUE);

			CuratorFramework curatorFramework = createZookeeperClient();

			{
				ZkClient zkClient = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(),
						standardCC.zkConnectionTimeoutMs(), new ZooKeeperStringSerializer());
				AdminUtils.createTopic(zkClient, topicName, 3, 2, new Properties());
				zkClient.close();
			}
				
			ZookeeperOffsetHandler.setOffsetInZooKeeper(curatorFramework, groupId, topicName, 0, offset);
	
			long fetchedOffset = ZookeeperOffsetHandler.getOffsetFromZooKeeper(curatorFramework, groupId, topicName, 0);

			curatorFramework.close();
			
			assertEquals(offset, fetchedOffset);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
