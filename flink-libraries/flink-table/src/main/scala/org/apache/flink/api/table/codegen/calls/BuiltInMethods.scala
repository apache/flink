package org.apache.flink.api.table.codegen.calls

import org.apache.calcite.linq4j.tree.Types
import org.apache.calcite.runtime.SqlFunctions

/**
  * @author dawid 
  */
object BuiltInMethods {
  val LOG10 = Types.lookupMethod(classOf[Math], "log10", classOf[Double])
  val EXP = Types.lookupMethod(classOf[Math], "exp", classOf[Double])
  val POWER = Types.lookupMethod(classOf[Math], "pow", classOf[Double], classOf[Double])
  val LN = Types.lookupMethod(classOf[Math], "log", classOf[Double])
  val ABS = Types.lookupMethod(classOf[SqlFunctions], "abs", classOf[Double])
}
