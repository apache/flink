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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for methods in the {@link KinesisProxy} class.
 */
public class KinesisProxyTest {

	@Test
	public void testIsRecoverableExceptionWithProvisionedThroughputExceeded() {
		final ProvisionedThroughputExceededException ex = new ProvisionedThroughputExceededException("asdf");
		ex.setErrorType(ErrorType.Client);
		assertTrue(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithServiceException() {
		final AmazonServiceException ex = new AmazonServiceException("asdf");
		ex.setErrorType(ErrorType.Service);
		assertTrue(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithExpiredIteratorException() {
		final ExpiredIteratorException ex = new ExpiredIteratorException("asdf");
		ex.setErrorType(ErrorType.Client);
		assertFalse(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithNullErrorType() {
		final AmazonServiceException ex = new AmazonServiceException("asdf");
		ex.setErrorType(null);
		assertFalse(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testCustomConfigurationOverride() {
		Properties configProps = new Properties();
		configProps.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		KinesisProxy proxy = new KinesisProxy(configProps) {
			@Override
			protected AmazonKinesis createKinesisClient(Properties configProps) {
				ClientConfiguration clientConfig = new ClientConfigurationFactory().getConfig();
				clientConfig.setSocketTimeout(10000);
				return AWSUtil.createKinesisClient(configProps, clientConfig);
			}
		};
		AmazonKinesis kinesisClient = Whitebox.getInternalState(proxy, "kinesisClient");
		ClientConfiguration clientConfiguration = Whitebox.getInternalState(kinesisClient, "clientConfiguration");
		assertEquals(10000, clientConfiguration.getSocketTimeout());
	}

	@Test
	public void testClientConfigOverride() {

		Properties configProps = new Properties();
		configProps.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		configProps.setProperty(AWSUtil.AWS_CLIENT_CONFIG_PREFIX + "socketTimeout", "9999");

		KinesisProxyInterface proxy = KinesisProxy.create(configProps);

		AmazonKinesis kinesisClient = Whitebox.getInternalState(proxy, "kinesisClient");
		ClientConfiguration clientConfiguration = Whitebox.getInternalState(kinesisClient,
			"clientConfiguration");
		assertEquals(9999, clientConfiguration.getSocketTimeout());
	}

}
