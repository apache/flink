package eu.stratosphere.streaming.api;

import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;

public final class StreamRecordProvider {
	public static Record addUUID(Record rec) {
		LongValue UUID = new LongValue(1);
		rec.addField(UUID);
		return rec;
	}
	
	public static Long popUUID(Record rec) {
		LongValue UUID = new LongValue(0);
		rec.getFieldInto(rec.getNumFields() - 1, UUID);
		rec.removeField(rec.getNumFields() - 1);
		return UUID.getValue();
	}
}
