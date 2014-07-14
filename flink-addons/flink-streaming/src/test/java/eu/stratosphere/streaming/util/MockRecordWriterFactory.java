package eu.stratosphere.streaming.util;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import org.mockito.Mockito;

import eu.stratosphere.streaming.api.streamcomponent.MockRecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class MockRecordWriterFactory {

	public static MockRecordWriter create() {
		MockRecordWriter recWriter = mock(MockRecordWriter.class);
		
		Mockito.when(recWriter.initList()).thenCallRealMethod();
		doCallRealMethod().when(recWriter).emit(Mockito.any(StreamRecord.class));
		
		recWriter.initList();
		
		return recWriter;
	}
}
