package eu.stratosphere.streaming.api;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.RecordWriter;

public class FaultTolerancyBufferTest {

	FaultTolerancyBuffer faultTolerancyBuffer;
	List<RecordWriter<StreamRecord>> outputs;

	@Before
	public void setFaultTolerancyBuffer() {
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		faultTolerancyBuffer = new FaultTolerancyBuffer(outputs, "1");
	}

	@Test
	public void testFaultTolerancyBuffer() {

		assertEquals(0, faultTolerancyBuffer.getNumberOfOutputs());
		assertEquals(outputs, faultTolerancyBuffer.getOutputs());
		assertEquals("1", faultTolerancyBuffer.getChannelID());

		faultTolerancyBuffer.setNumberOfOutputs(3);

		assertEquals(3, faultTolerancyBuffer.getNumberOfOutputs());

	}

	@Test
	public void testAddRecord() {
//		fail("Not yet implemented");
	}

	@Test
	public void testAddTimestamp() {
//		fail("Not yet implemented");
	}

	@Test
	public void testPopRecord() {
//		fail("Not yet implemented");
	}

	@Test
	public void testAckRecord() {
//		fail("Not yet implemented");
	}

	@Test
	public void testFailRecord() {
//		fail("Not yet implemented");
	}

	@Test
	public void testReEmit() {
//		fail("Not yet implemented");
	}

	@Test
	public void testGetRecordBuffer() {
//		fail("Not yet implemented");
	}

	@Test
	public void testTimeOutRecords() {
//		fail("Not yet implemented");
	}

}
