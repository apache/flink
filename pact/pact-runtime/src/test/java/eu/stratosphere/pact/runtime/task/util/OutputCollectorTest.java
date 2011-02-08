package eu.stratosphere.pact.runtime.task.util;

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class OutputCollectorTest {

	/*
	private class AbstractTaskMock extends AbstractTask {

		@Override
		public void invoke() throws Exception {
		}

		@Override
		public void registerInputOutput() {
		}
		
		@Override
		public Environment getEnvironment() {
			return null;
		}
	}
	*/
	
	/*
	private class RecordWriterMock<T extends Record> extends RecordWriter<T> {

		public T lastEmitted = null;
		
		public RecordWriterMock(Class<T> outputClass) {
			super(new AbstractTaskMock(), outputClass);
		}

		@Override
		public void emit(T record) {
			lastEmitted = record;
		}
		
		public T getLastEmitted() {
			return lastEmitted;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		private void connectOutputGate(Class<T> outputClass, ChannelSelector selector) {
			
		}
	}
	*/
	
	
	
	
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
	
	/*
	@Test
	public void testCollect() {
		
		OutputCollector<Key, Value> oc = new OutputCollector<Key, Value>();
		
		RecordWriterMock<KeyValuePair<Key, Value>> rwm1 = new RecordWriterMock(KeyValuePair.class);
		oc.addWriter(rwm1, false);
		RecordWriterMock<KeyValuePair<Key, Value>> rwm2 = new RecordWriterMock(KeyValuePair.class);
		oc.addWriter(rwm2, true);
		RecordWriterMock<KeyValuePair<Key, Value>> rwm3 = new RecordWriterMock(KeyValuePair.class);
		oc.addWriter(rwm3, true);
		RecordWriterMock<KeyValuePair<Key, Value>> rwm4 = new RecordWriterMock(KeyValuePair.class);
		oc.addWriter(rwm4, true);
		RecordWriterMock<KeyValuePair<Key, Value>> rwm5 = new RecordWriterMock(KeyValuePair.class);
		oc.addWriter(rwm5, false);
		RecordWriterMock<KeyValuePair<Key, Value>> rwm6 = new RecordWriterMock(KeyValuePair.class);
		oc.addWriter(rwm6, true);		
		
		KeyValuePair<PactInteger,PactInteger> testPair = new KeyValuePair<PactInteger, PactInteger>();
		
		testPair.setKey(new PactInteger(24));
		testPair.setValue(new PactInteger(987));
		
		oc.collect(testPair.getKey(), testPair.getValue());
		
		ArrayList<KeyValuePair<Key,Value>> distinctKeys = new ArrayList<KeyValuePair<Key,Value>>();
		distinctKeys.add(rwm2.getLastEmitted());
		distinctKeys.add(rwm3.getLastEmitted());
		distinctKeys.add(rwm4.getLastEmitted());
		distinctKeys.add(rwm6.getLastEmitted());
		
		Assert.assertTrue("Keys are not identical", 
			((PactInteger)rwm1.getLastEmitted().getKey()).getValue() == ((PactInteger)rwm2.getLastEmitted().getKey()).getValue());
		Assert.assertTrue("Values are not identical", 
			((PactInteger)rwm1.getLastEmitted().getValue()).getValue() == ((PactInteger)rwm2.getLastEmitted().getValue()).getValue());
		
		Assert.assertTrue("Keys are not identical", 
			((PactInteger)rwm1.getLastEmitted().getKey()).getValue() == ((PactInteger)rwm3.getLastEmitted().getKey()).getValue());
		Assert.assertTrue("Values are not identical", 
			((PactInteger)rwm1.getLastEmitted().getValue()).getValue() == ((PactInteger)rwm3.getLastEmitted().getValue()).getValue());
		
		Assert.assertTrue("Keys are not identical", 
			((PactInteger)rwm1.getLastEmitted().getKey()).getValue() == ((PactInteger)rwm4.getLastEmitted().getKey()).getValue());
		Assert.assertTrue("Values are not identical", 
			((PactInteger)rwm1.getLastEmitted().getValue()).getValue() == ((PactInteger)rwm4.getLastEmitted().getValue()).getValue());
		
		Assert.assertTrue("Keys are not identical", 
			((PactInteger)rwm1.getLastEmitted().getKey()).getValue() == ((PactInteger)rwm5.getLastEmitted().getKey()).getValue());
		Assert.assertTrue("Values are not identical", 
			((PactInteger)rwm1.getLastEmitted().getValue()).getValue() == ((PactInteger)rwm5.getLastEmitted().getValue()).getValue());
		
		Assert.assertTrue("Keys are not identical", 
			((PactInteger)rwm1.getLastEmitted().getKey()).getValue() == ((PactInteger)rwm6.getLastEmitted().getKey()).getValue());
		Assert.assertTrue("Values are not identical", 
			((PactInteger)rwm1.getLastEmitted().getValue()).getValue() == ((PactInteger)rwm6.getLastEmitted().getValue()).getValue());
		
		for(int i=0;i<distinctKeys.size();i++) {
			for(int j=i+1;j<distinctKeys.size();j++) {
				Assert.assertTrue("References are not different",distinctKeys.get(i) != distinctKeys.get(j));
			}
		}
		
	}
	*/
	
}
