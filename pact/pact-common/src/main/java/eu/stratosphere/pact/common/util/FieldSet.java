package eu.stratosphere.pact.common.util;

import java.util.HashSet;

public class FieldSet  extends HashSet<Integer>{

	private static final long serialVersionUID = -6333143284628275141L;

	public FieldSet() {
	}
	
	public FieldSet(int columnIndex) {
		this.add(columnIndex);
	}
	
	public FieldSet(int[] columnIndexes) {
		for (int columnIndex : columnIndexes)
			this.add(columnIndex);
	}
	
	public FieldSet(FieldSet o) {
		this.addAll(o);
	}
	
	public FieldSet(FieldSet o1, FieldSet o2) {
		this.addAll(o1);
		this.addAll(o2);
	}
}
