package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

@RunWith( PowerMockRunner.class )
@PrepareForTest( LibraryCacheManager.class )
public abstract class TaskTestBase {

	long memorySize = 0;
	MockEnvironment mockEnv;
	
	public void initEnvironment(long memorySize) {

		this.memorySize = memorySize;
		this.mockEnv = new MockEnvironment(this.memorySize);
		
		PowerMockito.mockStatic(LibraryCacheManager.class);
        try {
			Mockito.when(LibraryCacheManager.getClassLoader(null)).thenReturn(Thread.currentThread().getContextClassLoader());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void addInput(Iterator<KeyValuePair<PactInteger,PactInteger>> input) {
		this.mockEnv.addInput(input);
		new TaskConfig(mockEnv.getRuntimeConfiguration()).addInputShipStrategy(ShipStrategy.FORWARD);
	}
	
	public void addOutput(List<KeyValuePair<PactInteger,PactInteger>> output) {
		this.mockEnv.addOutput(output);
		new TaskConfig(mockEnv.getRuntimeConfiguration()).addOutputShipStrategy(ShipStrategy.FORWARD);
	}

	public TaskConfig getTaskConfig() {
		return new TaskConfig(mockEnv.getRuntimeConfiguration());
	}
	
	public void registerTask(AbstractTask task, Class<? extends Stub<PactInteger,PactInteger>> stubClass) {
		new TaskConfig(mockEnv.getRuntimeConfiguration()).setStubClass(stubClass);
		task.setEnvironment(mockEnv);
		task.registerInputOutput();
	}
	
	public void registerTask(AbstractTask task) {
		task.setEnvironment(mockEnv);
		task.registerInputOutput();
	}
	
	public MemoryManager getMemoryManager() {
		return mockEnv.getMemoryManager();
	}
	
	@After
	public void checkMemoryManager() throws Exception {
		if (this.memorySize > 0) {
			MemoryManager memMan = getMemoryManager();
			if (!memMan.verifyEmpty()) {
				Assert.fail("Memory Manager managed memory was not completely freed.");
			}
		}
	}
	
}
