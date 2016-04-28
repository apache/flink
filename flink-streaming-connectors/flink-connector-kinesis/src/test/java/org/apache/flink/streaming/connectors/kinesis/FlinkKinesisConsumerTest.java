/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.util.ReferenceKinesisShardTopologies;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FlinkKinesisConsumer.class)
public class FlinkKinesisConsumerTest {

	@Test
	public void testUnableToRetrieveShards() throws Exception {
		final KinesisProxy kinesisProxyMock = mock(KinesisProxy.class);
		whenNew(KinesisProxy.class).withArguments(props()).thenReturn(kinesisProxyMock);

		List<String> streamList = Collections.singletonList("flink-test");

		//when(kinesisProxyMock.getShardList(streamList)).thenReturn(Collections.<KinesisStreamShard>emptyList());
		when(kinesisProxyMock.getShardList(streamList)).thenReturn(ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards());

		new FlinkKinesisConsumer<>("flink-test", new SimpleStringSchema(), props());
	}

	private Properties props() {
		Properties props = new Properties();
		props.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		props.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		props.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		return props;
	}

}
