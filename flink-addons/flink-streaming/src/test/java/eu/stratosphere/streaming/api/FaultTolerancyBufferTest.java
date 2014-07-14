package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.types.StringValue;

public class FaultTolerancyBufferTest {

	FaultTolerancyBuffer faultTolerancyBuffer;
	List<RecordWriter<StreamRecord>> outputs;

	@Before
	public void setFaultTolerancyBuffer() {
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		faultTolerancyBuffer = new FaultTolerancyBuffer(outputs, "1");
		faultTolerancyBuffer.setNumberOfOutputs(3);
	}

	@Test
	public void testFaultTolerancyBuffer() {
		assertEquals(3, faultTolerancyBuffer.getNumberOfOutputs());
		assertEquals(outputs, faultTolerancyBuffer.getOutputs());
		assertEquals("1", faultTolerancyBuffer.getChannelID());
	}

	@Test
	public void testAddRecord() {
		StreamRecord record = (new StreamRecord(1)).setId("1");
		record.addRecord(new AtomRecord(new StringValue("V1")));
		faultTolerancyBuffer.addRecord(record);
		assertEquals((Integer) 3, faultTolerancyBuffer.getAckCounter().get(record.getId()));
		assertEquals(record,faultTolerancyBuffer.getRecordBuffer().get(record.getId()));
	}

	@Test
	public void testAddTimestamp() {
		Long cTime = System.currentTimeMillis();
		faultTolerancyBuffer.addTimestamp("1-1337");

		Long recordTimeStamp = faultTolerancyBuffer.getRecordTimestamps().get(
				"1-1337");

		assertTrue(recordTimeStamp - cTime < 2);

		String[] records = new String[] { "1-1337" };

		assertArrayEquals(records,
				faultTolerancyBuffer.getRecordsByTime().get(recordTimeStamp).toArray());
		
		try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		faultTolerancyBuffer.addTimestamp("1-1338");
		faultTolerancyBuffer.addTimestamp("1-1339");
		long recordTimeStamp1 = faultTolerancyBuffer.getRecordTimestamps().get(
				"1-1338");
		long recordTimeStamp2 = faultTolerancyBuffer.getRecordTimestamps().get(
				"1-1339");
		
		records = new String[] { "1-1338","1-1339"};
		
		if(recordTimeStamp1==recordTimeStamp2){
			assertTrue(faultTolerancyBuffer.getRecordsByTime().get(recordTimeStamp1).contains("1-1338"));
			assertTrue(faultTolerancyBuffer.getRecordsByTime().get(recordTimeStamp1).contains("1-1339"));
			assertTrue(faultTolerancyBuffer.getRecordsByTime().get(recordTimeStamp1).size()==2);
		}
		
		
		
	}

	@Test
	public void testPopRecord() {
		StreamRecord record1 = (new StreamRecord(1)).setId("1");
		record1.addRecord(new AtomRecord(new StringValue("V1")));
		faultTolerancyBuffer.addRecord(record1);
		
		assertEquals(record1, faultTolerancyBuffer.popRecord(record1.getId()));
		System.out.println("---------");
	}
	
	@Test
	public void testRemoveRecord() {
		StreamRecord record1 = (new StreamRecord(1)).setId("1");
		record1.addRecord(new AtomRecord(new StringValue("V1")));
		StreamRecord record2 = (new StreamRecord(1)).setId("1");
		record2.addRecord(new AtomRecord(new StringValue("V2")));
		
		faultTolerancyBuffer.addRecord(record1);
		faultTolerancyBuffer.addRecord(record2);
		
		Long record1TS=faultTolerancyBuffer.getRecordTimestamps().get(record1.getId());
		Long record2TS=faultTolerancyBuffer.getRecordTimestamps().get(record2.getId());
		
		faultTolerancyBuffer.removeRecord(record1.getId());
		assertTrue(faultTolerancyBuffer.getRecordBuffer().containsKey(record2.getId()));
		assertTrue(faultTolerancyBuffer.getAckCounter().containsKey(record2.getId()));
		assertTrue(faultTolerancyBuffer.getRecordTimestamps().containsKey(record2.getId()));
		assertTrue(faultTolerancyBuffer.getRecordsByTime().get(record2TS).contains(record2.getId()));
		
		assertFalse(faultTolerancyBuffer.getRecordBuffer().containsKey(record1.getId()));		
		assertFalse(faultTolerancyBuffer.getAckCounter().containsKey(record1.getId()));
		assertFalse(faultTolerancyBuffer.getRecordTimestamps().containsKey(record1.getId()));
		assertFalse(faultTolerancyBuffer.getRecordsByTime().get(record1TS).contains(record1.getId()));
		
	}

	@Test
	public void testAckRecord() {
		StreamRecord record1 = (new StreamRecord(1)).setId("1");
		record1.addRecord(new AtomRecord(new StringValue("V1")));
		faultTolerancyBuffer.addRecord(record1);
		Long record1TS=faultTolerancyBuffer.getRecordTimestamps().get(record1.getId());
		
		faultTolerancyBuffer.ackRecord(record1.getId());
		faultTolerancyBuffer.ackRecord(record1.getId());
		assertEquals((Integer) 1, faultTolerancyBuffer.getAckCounter().get(record1.getId()));
		assertTrue(faultTolerancyBuffer.getRecordBuffer().containsKey(record1.getId()));		
		assertTrue(faultTolerancyBuffer.getAckCounter().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getRecordTimestamps().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getRecordsByTime().get(record1TS).contains(record1.getId()));
		
		faultTolerancyBuffer.ackRecord(record1.getId());
		assertFalse(faultTolerancyBuffer.getRecordBuffer().containsKey(record1.getId()));		
		assertFalse(faultTolerancyBuffer.getAckCounter().containsKey(record1.getId()));
		assertFalse(faultTolerancyBuffer.getRecordTimestamps().containsKey(record1.getId()));
		assertFalse(faultTolerancyBuffer.getRecordsByTime().get(record1TS).contains(record1.getId()));
				
		faultTolerancyBuffer.ackRecord(record1.getId());
	}

	@Test
	public void testFailRecord() {
		StreamRecord record1 = (new StreamRecord(1)).setId("1");
		record1.addRecord(new AtomRecord(new StringValue("V1")));
		faultTolerancyBuffer.addRecord(record1);
		Long record1TS=faultTolerancyBuffer.getRecordTimestamps().get(record1.getId());
		
		assertTrue(faultTolerancyBuffer.getRecordBuffer().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getAckCounter().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getRecordTimestamps().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getRecordsByTime().get(record1TS).contains(record1.getId()));		
		
		String prevID = record1.getId();
	
		faultTolerancyBuffer.failRecord(record1.getId());

		Long record2TS=faultTolerancyBuffer.getRecordTimestamps().get(record1.getId());
		
		
		
		assertFalse(faultTolerancyBuffer.getRecordBuffer().containsKey(prevID));		
		assertFalse(faultTolerancyBuffer.getAckCounter().containsKey(prevID));
		assertFalse(faultTolerancyBuffer.getRecordTimestamps().containsKey(prevID));
		assertFalse(faultTolerancyBuffer.getRecordsByTime().get(record1TS).contains(prevID));		

		faultTolerancyBuffer.ackRecord(prevID);
		faultTolerancyBuffer.ackRecord(prevID);
		faultTolerancyBuffer.ackRecord(prevID);
		
		assertTrue(faultTolerancyBuffer.getRecordBuffer().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getAckCounter().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getRecordTimestamps().containsKey(record1.getId()));
		assertTrue(faultTolerancyBuffer.getRecordsByTime().get(record2TS).contains(record1.getId()));
		System.out.println("---------");
	}

	//TODO: create more tests for this method
	@Test
	public void testTimeOutRecords() {
		faultTolerancyBuffer.setTIMEOUT(1000);
		
		StreamRecord record1 = (new StreamRecord(1)).setId("1");
		record1.addRecord(new AtomRecord(new StringValue("V1")));
		StreamRecord record2 = (new StreamRecord(1)).setId("1");
		record2.addRecord(new AtomRecord(new StringValue("V2")));
		StreamRecord record3 = (new StreamRecord(1)).setId("1");
		record3.addRecord(new AtomRecord(new StringValue("V3")));
		
		faultTolerancyBuffer.addRecord(record1);
		faultTolerancyBuffer.addRecord(record2);
		
		try {
			Thread.sleep(500);
		} catch (Exception e) {
		}
		faultTolerancyBuffer.addRecord(record3);
		
		Long record1TS=faultTolerancyBuffer.getRecordTimestamps().get(record1.getId());
		Long record2TS=faultTolerancyBuffer.getRecordTimestamps().get(record2.getId());
				
		faultTolerancyBuffer.ackRecord(record1.getId());
		faultTolerancyBuffer.ackRecord(record1.getId());
		faultTolerancyBuffer.ackRecord(record1.getId());
		
		faultTolerancyBuffer.ackRecord(record2.getId());
		
		faultTolerancyBuffer.ackRecord(record3.getId());
		faultTolerancyBuffer.ackRecord(record3.getId());

				
		try {
			Thread.sleep(501);
		} catch (InterruptedException e) {
		}
		
		List<String> timedOutRecords = faultTolerancyBuffer.timeoutRecords(System.currentTimeMillis());
		
		System.out.println("timedOutRecords: "+ timedOutRecords);
		
		assertEquals(1, timedOutRecords.size());
		assertFalse(timedOutRecords.contains(record1.getId()));		
		assertFalse(faultTolerancyBuffer.getRecordsByTime().containsKey(record1TS));
		assertFalse(faultTolerancyBuffer.getRecordsByTime().containsKey(record2TS));
		
		assertTrue(faultTolerancyBuffer.getRecordBuffer().containsKey(record2.getId()));
		assertTrue(faultTolerancyBuffer.getAckCounter().containsKey(record2.getId()));
		assertTrue(faultTolerancyBuffer.getRecordTimestamps().containsKey(record2.getId()));
		
		System.out.println(faultTolerancyBuffer.getAckCounter());
		
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}
		
		timedOutRecords = faultTolerancyBuffer.timeoutRecords(System.currentTimeMillis());
		assertEquals(null,timedOutRecords);
		
		try {
			Thread.sleep(901);
		} catch (InterruptedException e) {
		}
		
		timedOutRecords = faultTolerancyBuffer.timeoutRecords(System.currentTimeMillis());
		System.out.println("timedOutRecords: "+ timedOutRecords);

		assertEquals(2, timedOutRecords.size());
		
		System.out.println(faultTolerancyBuffer.getAckCounter());
		System.out.println("---------");
	}
}
