package org.apache.flink.api.scala.extensions.acceptPartialFunctions.data

/**
  * Simple case class to test the `acceptPartialFunctions` extension
  * @param id A numerical identifier
  * @param value A textual value
  */
private[acceptPartialFunctions] case class KeyValuePair(id: Int, value: String)
