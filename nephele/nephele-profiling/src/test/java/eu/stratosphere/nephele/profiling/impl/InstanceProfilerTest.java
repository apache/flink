package eu.stratosphere.nephele.profiling.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;

/**
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(InstanceProfiler.class)
public class InstanceProfilerTest {

	@Mock
	private InstanceConnectionInfo infoMock;
	@Mock 
	private InetAddress addressMock;
	
	@Mock
	private BufferedReader bufferMock;
	
	private FileReader fileReaderMock;
	
	//object under test
	InstanceProfiler out; 
	
	@Before
	public void setUp() throws Exception
	{
		initMocks(this);
		when(this.infoMock.getAddress()).thenReturn(this.addressMock);
		when(this.addressMock.getHostAddress()).thenReturn("192.168.1.1");
		
		createBufferMock(bufferMock, InstanceProfiler.ETC_NETWORK_INTERFACES);
		createBufferMock(bufferMock, InstanceProfiler.VAR_LOG_DMESG);
		when(bufferMock.readLine()).thenReturn(
			"eth0", 
			"192.168.1.1",
			"[   24.160690] e1000e: eth0 NIC Link is Up 1000 Mbps Full Duplex, Flow Control: None",
			"cpu  222875 20767 209704 3782096 209864 0 1066 0 0 0",
			"MemTotal:        8052956 kB",
			"MemFree:         3999880 kB",
			"Buffers:           77216 kB",
			"Cached:          1929640 kB", 
			null,
			"  eth0: 364729203  286442    0    0    0     0          0      1060 14483806  191563    0    0    0     0       0          0",
			null
		);
		
		out = new InstanceProfiler(infoMock, false);
	}

	
	@Test
	public void shouldHaveNetworkTraffic() throws Exception
	{
		long now = System.currentTimeMillis();
		InternalInstanceProfilingData generateProfilingData = out.generateProfilingData(now);
		
		long receivedBytes = generateProfilingData.getReceivedBytes();
		assertThat(receivedBytes, is(equalTo(364729203L)));
		
		long transmittedBytes = generateProfilingData.getTransmittedBytes();
		assertThat(transmittedBytes, is(equalTo(14483806L)));
	}
	
	@Test
	public void shouldHaveNetworkMemSettingsMeasured() throws Exception
	{
		long now = System.currentTimeMillis();
		InternalInstanceProfilingData generateProfilingData = out.generateProfilingData(now);
		
		long totalMemory = generateProfilingData.getTotalMemory();
		assertThat(totalMemory, is(equalTo(8052956L)));
		
		long freeMemory = generateProfilingData.getFreeMemory();
		assertThat(freeMemory, is(equalTo(3999880L)));
		
		long buffers = generateProfilingData.getBufferedMemory();
		assertThat(buffers, is(equalTo(77216L)));
		
		long cached = generateProfilingData.getCachedMemory();
		assertThat(cached, is(equalTo(1929640L)));
	}
	
	@Test
	public void shouldMeasureCPUUtilization() throws Exception
	{
		//still code duplication, how can this be removed?
		long expectedCpuUser = 222875; 
		long expectedCpuNice = 20767; 
		long expectedCpuSys = 209704;
		long expectedCpuIdle = 3782096; 
		long expectedCpuIOWait = 209864;
		long expectedHardIrqCPU = 0L; 
		long expectedSoftIrqCpu = 1066;
		
		long deltaSum =  expectedCpuUser + expectedCpuNice + expectedCpuSys + expectedCpuIdle + expectedCpuIOWait + expectedHardIrqCPU + expectedSoftIrqCpu;
		
		long now = System.currentTimeMillis();
		InternalInstanceProfilingData generateProfilingData = out.generateProfilingData(now);
		
		long userCPU = generateProfilingData.getUserCPU();
		expectedCpuUser = (expectedCpuUser * 100) / deltaSum;
		assertThat(userCPU, is(equalTo(expectedCpuUser)));
		
		long idleCPU = generateProfilingData.getIdleCPU();
		expectedCpuIdle = (expectedCpuIdle * 100) / deltaSum;
		assertThat(idleCPU, is(equalTo(expectedCpuIdle)));
		
		long systemCPU = generateProfilingData.getSystemCPU();
		expectedCpuSys = (expectedCpuSys * 100) / deltaSum;
		assertThat(systemCPU, is(expectedCpuSys));
		
		long hardIrqCPU = generateProfilingData.getHardIrqCPU();
		expectedHardIrqCPU = (expectedHardIrqCPU * 100) / deltaSum;
		assertThat(hardIrqCPU, is(equalTo(expectedHardIrqCPU)));
		
		long softIrqCPU = generateProfilingData.getSoftIrqCPU();
		expectedSoftIrqCpu = (expectedSoftIrqCpu * 100) / deltaSum;
		assertThat(softIrqCPU, is(equalTo(expectedSoftIrqCpu)));
		
		long ioWaitCPU = generateProfilingData.getIOWaitCPU();
		expectedCpuIOWait = (expectedCpuIOWait * 100) / deltaSum;
		assertThat(ioWaitCPU, is(equalTo(expectedCpuIOWait)));
	}
	
	private void createBufferMock(BufferedReader mock, String fileString) throws FileNotFoundException, Exception {
		
		whenNew(FileReader.class).withArguments(fileString).thenReturn(this.fileReaderMock);
		whenNew(BufferedReader.class).withArguments(this.fileReaderMock).thenReturn(mock);
	}
	
}
