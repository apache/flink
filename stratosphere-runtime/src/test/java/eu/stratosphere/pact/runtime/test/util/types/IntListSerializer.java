package eu.stratosphere.pact.runtime.test.util.types;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

public class IntListSerializer extends TypeSerializer<IntList> {

	@Override
	public IntList createInstance() {
		return new IntList();
	}
	
	@Override
	public IntList copy(IntList from, IntList reuse) {
		reuse.setKey(from.getKey());
		reuse.setValue(from.getValue());
		return reuse;
	}
	
	public IntList createCopy(IntList from) {
		return new IntList(from.getKey(), from.getValue());
	}

	public void copyTo(IntList from, IntList to) {
		to.setKey(from.getKey());
		to.setValue(from.getValue());
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(IntList record, DataOutputView target) throws IOException {
		target.writeInt(record.getKey());
		target.writeInt(record.getValue().length);
		for (int i = 0; i < record.getValue().length; i++) {
				target.writeInt(record.getValue()[i]);
		}
	}

	@Override
	public IntList deserialize(IntList record, DataInputView source) throws IOException {
		int key = source.readInt();
		record.setKey(key);
		int size = source.readInt();
		int[] value = new int[size];
		for (int i = 0; i < value.length; i++) {
			value[i] = source.readInt();
		}
		record.setValue(value);
		return record;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeInt(source.readInt());
		int len = source.readInt();
		target.writeInt(len);
		for (int i = 0; i < len; i++) {
			target.writeInt(source.readInt());
		}
	}

}
