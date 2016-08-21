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
package org.apache.flink.client.program;

import akka.dispatch.Futures;
import org.apache.flink.api.common.JobClient;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobListeningContext;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.SerializedValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import scala.concurrent.Promise;

import java.util.Collections;


/**
 * Tests the JobClient implementations.
 *
 * See also: JobRetrievalITCase
 */
public class JobClientTest {

	private static boolean finalizeCalled;

	private JobListeningContext listeningContext;
	private JobID jobID;
	private JobManagerMessages.JobResultSuccess successMessage;

	private Runnable finalizer = new Runnable() {
		@Override
		public void run() {
			finalizeCalled = true;
		}
	};

	private Promise<Object> resultPromise;

	@Before
	public void beforeTest() throws Exception {
		finalizeCalled = false;

		this.jobID = JobID.generate();
		this.listeningContext = Mockito.mock(JobListeningContext.class);
		this.resultPromise = Futures.promise();
		ActorGateway mockActorClientGateway = Mockito.mock(ActorGateway.class);
		Mockito.when(listeningContext.getJobID()).thenReturn(jobID);
		Mockito.when(listeningContext.getJobClientGateway()).thenReturn(mockActorClientGateway);
		Mockito.when(listeningContext.getJobResultFuture()).thenReturn(resultPromise.future());
		Mockito.when(listeningContext.getClassLoader()).thenReturn(JobClientTest.class.getClassLoader());

		this.successMessage = new JobManagerMessages.JobResultSuccess(
			new SerializedJobExecutionResult(
				jobID,
				42,
				Collections.singletonMap("key", new SerializedValue<Object>("value"))));
	}

	@Test(timeout = 10000)
	public void testEagerJobClient() throws Exception {

		JobClient jobClient = new JobClientEager(listeningContext);

		jobClient.addFinalizer(finalizer);

		Assert.assertFalse(jobClient.hasFinished());

		resultPromise.success(successMessage);

		Assert.assertTrue(jobClient.hasFinished());

		JobExecutionResult retrievedResult = jobClient.waitForResult();
		Assert.assertNotNull(retrievedResult);

		Assert.assertEquals(jobID, retrievedResult.getJobID());
		Assert.assertEquals(42, retrievedResult.getNetRuntime());
		Assert.assertEquals(1, retrievedResult.getAllAccumulatorResults().size());
		Assert.assertEquals("value", retrievedResult.getAllAccumulatorResults().get("key"));

		jobClient.shutdown();
		Assert.assertTrue(finalizeCalled);

		finalizeCalled = false;
		jobClient.shutdown();
		Assert.assertFalse(finalizeCalled);
	}

	@Test(timeout = 10000)
	public void testLazyJobClient() throws Exception {

		ClusterClient mockedClusterClient = Mockito.mock(ClusterClient.class);
		JobClientEager eagerJobClient = new JobClientEager(listeningContext);
		Mockito.when(mockedClusterClient.retrieveJob(jobID))
			.thenReturn(eagerJobClient);

		JobClient jobClient = new JobClientLazy(jobID, mockedClusterClient);
		jobClient.addFinalizer(finalizer);

		Assert.assertFalse(jobClient.hasFinished());

		resultPromise.success(successMessage);

		Assert.assertTrue(jobClient.hasFinished());

		JobExecutionResult retrievedResult = jobClient.waitForResult();
		Assert.assertNotNull(retrievedResult);

		Assert.assertEquals(jobID, retrievedResult.getJobID());
		Assert.assertEquals(42, retrievedResult.getNetRuntime());
		Assert.assertEquals(1, retrievedResult.getAllAccumulatorResults().size());
		Assert.assertEquals("value", retrievedResult.getAllAccumulatorResults().get("key"));

		jobClient.shutdown();
		Assert.assertTrue(finalizeCalled);

		finalizeCalled = false;
		jobClient.shutdown();
		Assert.assertFalse(finalizeCalled);
	}

}
