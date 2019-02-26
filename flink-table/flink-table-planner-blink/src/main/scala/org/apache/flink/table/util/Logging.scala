package org.apache.flink.table.util

import org.slf4j.{Logger, LoggerFactory}

/**
  * Helper class to ensure the logger is never serialized.
  */
trait Logging {
  @transient lazy val LOG: Logger = LoggerFactory.getLogger(getClass)
}
