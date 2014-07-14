package eu.stratosphere.streaming.api;

import java.util.UUID;

import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;

public final class StreamRecordProvider {
	public static Long addUUID(Record rec) {
		UUID uuid = UUID.randomUUID();
		rec.addField(new LongValue(uuid.getMostSignificantBits()));
		return uuid.getLeastSignificantBits();
	}
	
	public static Long popUUID(Record rec) {
		LongValue mostSignificantBits = new LongValue();
		rec.getFieldInto(rec.getNumFields() - 1, mostSignificantBits);
		rec.removeField(rec.getNumFields() - 1);
		return mostSignificantBits.getValue();
	}
}
