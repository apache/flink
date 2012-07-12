package eu.stratosphere.pact.runtime.shipping;

/**
 * Enumeration defining the different shipping types of the output, such as local forward, re-partitioning by hash,
 * or re-partitioning by range.
 */
public enum ShipStrategy {
	FORWARD,
	PARTITION_HASH,
	PARTITION_LOCAL_HASH,
	PARTITION_RANGE,
	PARTITION_LOCAL_RANGE,
	BROADCAST,
	SFR,
	NONE
}