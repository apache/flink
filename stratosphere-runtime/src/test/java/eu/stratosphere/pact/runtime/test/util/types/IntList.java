package eu.stratosphere.pact.runtime.test.util.types;

public class IntList {
	
	private int key;
	private int[] value;

	public IntList() {}
	
	public IntList(int key, int[] value) {
		this.key = key;
		this.value = value;
	}
	
	
	public int getKey() {
		return key;
	}
	
	public void setKey(int key) {
		this.key = key;
	}
	
	public int[] getValue() {
		return value;
	}
	
	public void setValue(int[] value) {
		this.value = value;
	}
	
	/**
	 * returns record size when serialized in bytes
	 * 
	 * @return size in bytes
	 */
	public int getSerializedSize() {
		return (2 + this.value.length) * 4;
	}
	
	@Override
	public String toString() {
		String result = "( " + this.key + " / ";
		for (int i = 0; i < value.length; i++) {
			result = result + value[i] + " "; 
		}
		return result + ")";
	}
}
