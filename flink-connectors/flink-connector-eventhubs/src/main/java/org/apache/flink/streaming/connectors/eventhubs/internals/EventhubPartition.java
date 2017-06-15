package org.apache.flink.streaming.connectors.eventhubs.internals;

import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Properties;

/**
 * Created by jozh on 5/23/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 */

public class EventhubPartition implements Serializable {
	private static final long serialVersionUID = 134878919919793479L;
	private final int cachedHash;
	private final String policyName;
	private final String policyKey;
	private final String namespace;
	private final String name;

	public int getParitionId() {
		return paritionId;
	}

	public String getPartitionName(){
		return namespace + "-" + name;
	}

	private final int paritionId;

	public EventhubPartition(Properties props, int parition){
		this(props.getProperty("eventhubs.policyname"),
			props.getProperty("eventhubs.policykey"),
			props.getProperty("eventhubs.namespace"),
			props.getProperty("eventhubs.name"),
			parition);
	}

	public EventhubPartition(String policyName, String policyKey, String namespace, String name, int paritionId){
		Preconditions.checkArgument(paritionId >= 0);

		this.policyName = Preconditions.checkNotNull(policyName);
		this.policyKey = Preconditions.checkNotNull(policyKey);
		this.name = Preconditions.checkNotNull(name);
		this.namespace = Preconditions.checkNotNull(namespace);
		this.paritionId = paritionId;
		this.cachedHash = 31 * (this.namespace + this.name).hashCode() + paritionId;
	}

	@Override
	public String toString() {
		return "EventhubPartition, namespace: " + this.namespace +
			" name: " + this.name +
			" partition: " + this.paritionId;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EventhubPartition){
			return this.hashCode() == ((EventhubPartition) obj).hashCode();
		}

		return false;
	}

	@Override
	public int hashCode() {
		return this.cachedHash;
	}
}
