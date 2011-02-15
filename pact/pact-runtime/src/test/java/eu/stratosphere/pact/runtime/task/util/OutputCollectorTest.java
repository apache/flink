package eu.stratosphere.pact.runtime.task.util;

import java.io.IOException;
import java.util.HashSet;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class OutputCollectorTest {
	
	@Test 
	public void testAddWriter() {
		OutputCollector<Key, Value> oc = new OutputCollector<Key,Value>();
		
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 0);
		oc.addWriter(null, true);  //   1 ->   1
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 1);
		oc.addWriter(null, false); //   2 ->   0
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 1);
		oc.addWriter(null, false); //   4 ->   0
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 1);
		oc.addWriter(null, true);  //   8 ->   8
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 9);
		oc.addWriter(null, true);  //  16 ->  16
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 25);
		oc.addWriter(null, false); //  32 ->   0
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 25);
		oc.addWriter(null, false); //  64 ->   0
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 25);
		oc.addWriter(null, true);  // 128 -> 128
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 153);
		oc.addWriter(null, true);  // 256 -> 256
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 409);
		oc.addWriter(null, true);  // 512 -> 512
		Assert.assertTrue("Copy-flag bitmask not correctly computed", oc.fwdCopyFlags == 921);
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testCollect() {

		Class<RecordWriter<KeyValuePair<PactInteger,PactInteger>>> rwC = (Class<RecordWriter<KeyValuePair<PactInteger,PactInteger>>>)((Class<?>)RecordWriter.class);
		Class<KeyValuePair<PactInteger,PactInteger>> kvpC = (Class<KeyValuePair<PactInteger,PactInteger>>)((Class<?>)KeyValuePair.class);
		
		// create readers
		RecordWriter<KeyValuePair<PactInteger,PactInteger>> rwMock1 = new RecordWriter<KeyValuePair<PactInteger,PactInteger>>(new MockTask(), kvpC); 
		RecordWriter<KeyValuePair<PactInteger,PactInteger>> rwMock2 = new RecordWriter<KeyValuePair<PactInteger,PactInteger>>(new MockTask(), kvpC);
		RecordWriter<KeyValuePair<PactInteger,PactInteger>> rwMock3 = new RecordWriter<KeyValuePair<PactInteger,PactInteger>>(new MockTask(), kvpC);
		RecordWriter<KeyValuePair<PactInteger,PactInteger>> rwMock4 = new RecordWriter<KeyValuePair<PactInteger,PactInteger>>(new MockTask(), kvpC);
		RecordWriter<KeyValuePair<PactInteger,PactInteger>> rwMock5 = new RecordWriter<KeyValuePair<PactInteger,PactInteger>>(new MockTask(), kvpC);
		RecordWriter<KeyValuePair<PactInteger,PactInteger>> rwMock6 = new RecordWriter<KeyValuePair<PactInteger,PactInteger>>(new MockTask(), kvpC);
		// mock readers
		rwMock1 = Mockito.mock(rwC);
		rwMock2 = Mockito.mock(rwC);
		rwMock3 = Mockito.mock(rwC);
		rwMock4 = Mockito.mock(rwC);
		rwMock5 = Mockito.mock(rwC);
		rwMock6 = Mockito.mock(rwC);
		
		ArgumentCaptor<KeyValuePair<PactInteger,PactInteger>> captor1 = ArgumentCaptor.forClass(kvpC);
		ArgumentCaptor<KeyValuePair<PactInteger,PactInteger>> captor2 = ArgumentCaptor.forClass(kvpC);
		ArgumentCaptor<KeyValuePair<PactInteger,PactInteger>> captor3 = ArgumentCaptor.forClass(kvpC);
		ArgumentCaptor<KeyValuePair<PactInteger,PactInteger>> captor4 = ArgumentCaptor.forClass(kvpC);
		ArgumentCaptor<KeyValuePair<PactInteger,PactInteger>> captor5 = ArgumentCaptor.forClass(kvpC);
		ArgumentCaptor<KeyValuePair<PactInteger,PactInteger>> captor6 = ArgumentCaptor.forClass(kvpC);
		
		OutputCollector<PactInteger, PactInteger> oc = new OutputCollector<PactInteger, PactInteger>();
		
		oc.addWriter(rwMock1, false);
		oc.addWriter(rwMock2, true);
		oc.addWriter(rwMock3, true);
		oc.addWriter(rwMock4, true);
		oc.addWriter(rwMock5, false);
		oc.addWriter(rwMock6, true);	

		oc.collect(new PactInteger(1), new PactInteger(123));
		oc.collect(new PactInteger(23), new PactInteger(672));
		oc.collect(new PactInteger(1673), new PactInteger(-12));

		try {
			Mockito.verify(rwMock1, Mockito.times(3)).emit(captor1.capture());
			Mockito.verify(rwMock2, Mockito.times(3)).emit(captor2.capture());
			Mockito.verify(rwMock3, Mockito.times(3)).emit(captor3.capture());
			Mockito.verify(rwMock4, Mockito.times(3)).emit(captor4.capture());
			Mockito.verify(rwMock5, Mockito.times(3)).emit(captor5.capture());
			Mockito.verify(rwMock6, Mockito.times(3)).emit(captor6.capture());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		HashSet<Integer> refs = new HashSet<Integer>();
		
		// first pair
		refs.add(System.identityHashCode(captor2.getAllValues().get(0)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor3.getAllValues().get(0))));
		refs.add(System.identityHashCode(captor3.getAllValues().get(0)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor4.getAllValues().get(0))));
		refs.add(System.identityHashCode(captor4.getAllValues().get(0)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor6.getAllValues().get(0))));
		refs.add(System.identityHashCode(captor6.getAllValues().get(0)));
		
		Assert.assertTrue(refs.contains(System.identityHashCode(captor5.getAllValues().get(0))));
		
		refs.clear();

		// second pair
		refs.add(System.identityHashCode(captor2.getAllValues().get(1)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor3.getAllValues().get(1))));
		refs.add(System.identityHashCode(captor3.getAllValues().get(1)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor4.getAllValues().get(1))));
		refs.add(System.identityHashCode(captor4.getAllValues().get(1)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor6.getAllValues().get(1))));
		refs.add(System.identityHashCode(captor6.getAllValues().get(1)));
		
		Assert.assertTrue(refs.contains(System.identityHashCode(captor5.getAllValues().get(1))));
		
		refs.clear();
		
		// third pair
		refs.add(System.identityHashCode(captor2.getAllValues().get(2)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor3.getAllValues().get(2))));
		refs.add(System.identityHashCode(captor3.getAllValues().get(2)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor4.getAllValues().get(2))));
		refs.add(System.identityHashCode(captor4.getAllValues().get(2)));
		Assert.assertFalse(refs.contains(System.identityHashCode(captor6.getAllValues().get(2))));
		refs.add(System.identityHashCode(captor6.getAllValues().get(2)));
		
		Assert.assertTrue(refs.contains(System.identityHashCode(captor5.getAllValues().get(2))));
		
		refs.clear();
	}
	
	private class MockTask extends AbstractTask {

		@Override
		public void invoke() throws Exception {
		}

		@Override
		public void registerInputOutput() {
		}
		
		@Override
		public Environment getEnvironment() {
			return new MockEnvironment();
		}
	}
	
	private class MockEnvironment extends Environment {
		
		@Override
		public int getNumberOfOutputGates() {
			return 0;
		}
	}
	
}
