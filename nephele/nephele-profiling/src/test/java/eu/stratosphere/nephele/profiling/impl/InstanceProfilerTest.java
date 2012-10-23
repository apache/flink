/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.Inet4Address;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;

/**
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(InstanceProfiler.class)
public class InstanceProfilerTest {

	private InstanceConnectionInfo ici;

	@Mock
	private BufferedReader cpuBufferMock;

	@Mock
	private BufferedReader networkBufferMock;

	@Mock
	private BufferedReader memoryBufferMock;

	@Mock
	private FileReader cpuReaderMock;

	@Mock
	private FileReader networkReaderMock;

	@Mock
	private FileReader memoryReaderMock;

	// object under test
	InstanceProfiler out;

	@Before
	public void setUp() throws Exception {

		this.ici = new InstanceConnectionInfo(Inet4Address.getByName("localhost"), 1234, 1235);

		initMocks(this);

		whenNew(FileReader.class).withArguments(InstanceProfiler.PROC_STAT).thenReturn(this.cpuReaderMock);
		whenNew(BufferedReader.class).withArguments(this.cpuReaderMock).thenReturn(this.cpuBufferMock);

		when(this.cpuBufferMock.readLine()).thenReturn(
			"cpu  222875 20767 209704 3782096 209864 0 1066 0 0 0"
			);

		whenNew(FileReader.class).withArguments(InstanceProfiler.PROC_NET_DEV).thenReturn(this.networkReaderMock);
		whenNew(BufferedReader.class).withArguments(this.networkReaderMock).thenReturn(this.networkBufferMock);

		when(this.networkBufferMock.readLine())
			.thenReturn(
				"  eth0: 364729203  286442    0    0    0     0          0      1060 14483806  191563    0    0    0     0       0          0",
				(String) null,
				"  eth0: 364729203  286442    0    0    0     0          0      1060 14483806  191563    0    0    0     0       0          0",
				(String) null,
				"  eth0: 364729203  286442    0    0    0     0          0      1060 14483806  191563    0    0    0     0       0          0",
				(String) null
			);

		whenNew(FileReader.class).withArguments(InstanceProfiler.PROC_MEMINFO).thenReturn(this.memoryReaderMock);
		whenNew(BufferedReader.class).withArguments(this.memoryReaderMock).thenReturn(this.memoryBufferMock);

		when(this.memoryBufferMock.readLine()).thenReturn(
			"MemTotal:        8052956 kB",
			"MemFree:         3999880 kB",
			"Buffers:           77216 kB",
			"Cached:          1929640 kB",
			null,
			"MemTotal:        8052956 kB",
			"MemFree:         3999880 kB",
			"Buffers:           77216 kB",
			"Cached:          1929640 kB",
			null,
			"MemTotal:        8052956 kB",
			"MemFree:         3999880 kB",
			"Buffers:           77216 kB",
			"Cached:          1929640 kB",
			null
			);

		PowerMockito.mockStatic(System.class);
		when(System.currentTimeMillis()).thenReturn(0L);

		this.out = new InstanceProfiler(this.ici);
	}

	@Test
	public void shouldHaveNetworkTraffic() {

		try {
			final InternalInstanceProfilingData generateProfilingData = out.generateProfilingData(0L);
			assertEquals(0L, generateProfilingData.getReceivedBytes());
			assertEquals(0L, generateProfilingData.getTransmittedBytes());
		} catch (ProfilingException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void shouldHaveMemSettingsMeasured() {

		try {
			final InternalInstanceProfilingData generateProfilingData = out.generateProfilingData(0L);

			final long totalMemory = generateProfilingData.getTotalMemory();
			assertThat(totalMemory, is(equalTo(8052956L)));

			long freeMemory = generateProfilingData.getFreeMemory();
			assertThat(freeMemory, is(equalTo(3999880L)));

			long buffers = generateProfilingData.getBufferedMemory();
			assertThat(buffers, is(equalTo(77216L)));

			long cached = generateProfilingData.getCachedMemory();
			assertThat(cached, is(equalTo(1929640L)));
		} catch (ProfilingException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void shouldMeasureCPUUtilization() {

		try {
			final InternalInstanceProfilingData generateProfilingData = out.generateProfilingData(0L);

			assertEquals(0L, generateProfilingData.getUserCPU());
			assertEquals(0L, generateProfilingData.getIdleCPU());
			assertEquals(0L, generateProfilingData.getSystemCPU());
			assertEquals(0L, generateProfilingData.getHardIrqCPU());
			assertEquals(0L, generateProfilingData.getSoftIrqCPU());
			assertEquals(0L, generateProfilingData.getIOWaitCPU());
		} catch (ProfilingException e) {
			fail(e.getMessage());
		}
	}
}
