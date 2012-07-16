package eu.stratosphere.pact.common.util;

import java.util.ArrayList;

public class FieldList  extends ArrayList<Integer>{

	private static final long serialVersionUID = -6333143284628275141L;

	public FieldList() {
	}
	
	public FieldList(int columnIndex) {
		super(1);
		this.add(columnIndex);
	}
	
	public FieldList(int[] columnIndexes) {
		super(columnIndexes.length);
		for(int i=0;i<columnIndexes.length;i++) {
			if(this.contains(columnIndexes[i])) {
				throw new IllegalArgumentException("Fields must be unique");
			}
			this.add(columnIndexes[i]);
		}
	}
	
}
