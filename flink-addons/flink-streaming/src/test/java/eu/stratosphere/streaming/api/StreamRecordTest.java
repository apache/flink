package eu.stratosphere.streaming.api;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.types.StringValue;

public class StreamRecordTest {

	@Test
	public void copyTest() {
		StreamRecord a = new StreamRecord(new StringValue("Big"));
		StreamRecord b = a.copy();
		assertTrue(((StringValue) a.getField(0)).getValue().equals(((StringValue) b.getField(0)).getValue()));
		b.setRecord(new StringValue("Data"));
		assertFalse(((StringValue) a.getField(0)).getValue().equals(((StringValue) b.getField(0)).getValue()));
	}
	
	@Test
	public void exceptionTest() {
		StreamRecord a = new StreamRecord(new StringValue("Big"));
		try {
			a.setRecord(4, new StringValue("Data"));
			fail();
		}
		catch (NoSuchRecordException e) {
		}
		
		try {
			a.setRecord(new StringValue("Data"), new StringValue("Stratosphere"));
			fail();
		}
		catch (RecordSizeMismatchException e) {
		}
	}

}
