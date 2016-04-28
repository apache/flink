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

import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

public class FlinkKinesisConsumerConfigValidationTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testMissingAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("AWS region must be set");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testUnrecognizableAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS region");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "wrongRegionId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testCredentialProviderTypeDefaultToBasicButNoCredentialsSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Need to set values for AWS Access Key ID and Secret Key");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Need to set values for AWS Access Key ID and Secret Key");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "BASIC");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testUnrecognizableCredentialProviderTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS Credential Provider Type");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "wrongProviderType");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testUnrecognizableStreamInitPositionTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid initial position in stream");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "BASIC");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "wrongInitPosition");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

}
