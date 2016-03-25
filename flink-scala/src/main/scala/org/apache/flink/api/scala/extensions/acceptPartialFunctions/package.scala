package org.apache.flink.api.scala.extensions

/**
  * acceptPartialFunctions extends the original DataSet with methods with unique names
  * that delegate to core higher-order functions (e.g. `map`) so that we can work around
  * the fact that overloaded methods taking functions as parameters can't accept partial
  * functions as well. This enables the possibility to directly apply pattern matching
  * to decompose inputs such as tuples, case classes and collections.
  *
  * e.g.
  * {{{
  *   object Main {
  *     import org.apache.flink.api.scala.extensions._
  *     case class Point(x: Double, y: Double)
  *     def main(args: Array[String]): Unit = {
  *       val env = ExecutionEnvironment.getExecutionEnvironment
  *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
  *       ds.filterWith {
  *         case Point(x, _) => x > 1
  *       }.reduceWith {
  *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
  *       }.mapWith {
  *         case Point(x, y) => (x, y)
  *       }.flatMapWith {
  *         case (x, y) => Seq('x' -> x, 'y' -> y)
  *       }.groupingBy {
  *         case (id, value) => id
  *       }
  *     }
  *   }
  * }}}
  *
  */
package object acceptPartialFunctions {

  // Empty: this package object purpose is just to document the package (see Scaladoc above)

}
