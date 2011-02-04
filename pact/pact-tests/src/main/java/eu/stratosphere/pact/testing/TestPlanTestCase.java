package eu.stratosphere.pact.testing;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.ipc.RPC;

/**
 * Base class for {@link TestPlan} test cases.
 * 
 * @author Arvid Heise
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ RPC.class, LibraryCacheManager.class })
public class TestPlanTestCase {
	private static final ClosableManager CLOSABLE_MANAGER = new ClosableManager();

	static void addTestPlan(TestPlan plan) {
		CLOSABLE_MANAGER.add(plan);
	}

	/**
	 * Mocks RPC communications
	 */
	@Before
	public final void setupMocking() {
		final Configuration config = new Configuration();
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY,
				"localhost");
		GlobalConfiguration.includeConfiguration(config);

		mockStatic(RPC.class, new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				if (invocation.getMethod().getName().equals("getProxy"))
					return MockJobManager.getInstance();
				else if (invocation.getMethod().getName().equals("stopProxy"))
					return null;
				return invocation.callRealMethod();
			}
		});
		mockStatic(LibraryCacheManager.class, new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				if (invocation.getMethod().getName().equals("getClassLoader"))
					return TestPlanTestCase.class.getClassLoader();
				return invocation.callRealMethod();
			}
		});
		// try {
		// when(
		// RPC.getProxy(Matchers.any(Class.class),
		// Matchers.any(InetSocketAddress.class),
		// Matchers.any(SocketFactory.class))).thenReturn(new MockJobManager());
		// verifyStatic();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// replay(RPC.class);
	}

	/**
	 * Closes all resources, especially file-based sort merger.
	 */
	@After
	public void closeClosables() {
		try {
			System.err.println("before cleanup: " + MockTaskManager.INSTANCE.getMemoryManager().toString());
			CLOSABLE_MANAGER.close();
			System.err.println("after cleanup: " + MockTaskManager.INSTANCE.getMemoryManager().toString());
		} catch (IOException e) {
			Assert.fail(e.toString());
		}
	}
}
