package org.apache.flink.streaming.connectors.hbase.example;

/**
 * Pojo for flink hbase example.
 */
public class Message {

	private String key;
	private Integer value;
	private Integer oldValue;

	public Message() {
		this(null, null, null);
	}

	public Message(String key, Integer value, Integer oldValue) {
		this.key = key;
		this.value = value;
		this.oldValue = oldValue;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	public Integer getOldValue() {
		return oldValue;
	}

	public void setOldValue(Integer oldValue) {
		this.oldValue = oldValue;
	}
}
