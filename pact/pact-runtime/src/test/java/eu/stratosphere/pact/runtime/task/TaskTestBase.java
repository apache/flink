package eu.stratosphere.pact.runtime.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

@RunWith( PowerMockRunner.class )
@PrepareForTest( LibraryCacheManager.class )
public abstract class TaskTestBase {

	MockEnvironment mockEnv;
	
	public void setupEnvironment() {
		
		this.mockEnv = new MockEnvironment();
		
		PowerMockito.mockStatic(LibraryCacheManager.class);
        try {
			Mockito.when(LibraryCacheManager.getClassLoader(null)).thenReturn(Thread.currentThread().getContextClassLoader());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void addInput(Iterator<KeyValuePair<PactInteger,PactInteger>> input) {
		this.mockEnv.addInput(input);
	}

	public TaskConfig getTaskConfig() {
		return new TaskConfig(mockEnv.getRuntimeConfiguration());
	}
	
	public void registerTask(AbstractTask task) {
		task.setEnvironment(mockEnv);
		task.registerInputOutput();
	}
	
	public Iterator<KeyValuePair<PactInteger, PactInteger>> createInputIterator(int[] keys, int[] values) {
		
		if(keys.length != values.length) return null;
		ArrayList<KeyValuePair<PactInteger,PactInteger>> pairList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();
		
		for(int i=0;i<keys.length;i++) {
			pairList.add(new KeyValuePair<PactInteger, PactInteger>(new PactInteger(keys[i]), new PactInteger(values[i])));
		}
		
		return pairList.iterator();
	}
	
	
	
}
