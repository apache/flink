package org.apache.flink.streaming.api.windowing.triggers

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window

object TriggerOf {

  import org.apache.flink.streaming.api.windowing.triggers.TriggerResult._

  /** Combine two [[TriggerResult]] values. This is a monoidal operation.
   *
   * @param r1 The first [[TriggerResult]] value.
   * @param r2 The second [[TriggerResult]] value.
   * @return A new [[TriggerResult]] value that combines the two values.
   */
  def or(r1: TriggerResult, r2: TriggerResult): TriggerResult = (r1, r2) match {
    case (CONTINUE, x) ⇒ x
    case (x, CONTINUE) ⇒ x
    case (_, FIRE_AND_PURGE) | (FIRE_AND_PURGE, _) ⇒ FIRE_AND_PURGE
    case (PURGE, FIRE) | (FIRE, PURGE) ⇒ FIRE_AND_PURGE // This could also be defined as `FIRE` or as `PURGE` without violating the monoid associativity law.
    case (PURGE, PURGE) ⇒ PURGE
    case (FIRE, FIRE) ⇒ FIRE
  }

  // Syntax extension for the combining operation for [[TriggerResult]] values.
  implicit class OrOps(val r1: TriggerResult) extends AnyVal {
    def \/(r2: TriggerResult): TriggerResult = or(r1, r2)
  }

}

/** Combine two triggers into one. The new trigger fires whenever one of the two previously defined triggers fire.
 * Both triggers must have the same type of window and the same type of stream elements.
 *
 * The firing events (`CONTINUE`, `FIRE`, `PURGE`, `FIRE_AND_PURGE`) are combined according to the monoid operation `\/` defined above.
 *
 * @param t1 A first trigger.
 * @param t2 A second trigger.
 * @tparam T The type of stream elements.
 * @tparam W The type of window.
 */
final case class TriggerOf[T, W <: Window](t1: Trigger[T, W], t2: Trigger[T, W]) extends Trigger[T, W] {

  import TriggerOf._

  override def onElement(element: T, timestamp: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = t1.onElement(element, timestamp, window, ctx) \/ t2.onElement(element, timestamp, window, ctx)

  override def onProcessingTime(time: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = t1.onProcessingTime(time, window, ctx) \/ t2.onProcessingTime(time, window, ctx)

  override def onEventTime(time: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = t1.onEventTime(time, window, ctx) \/ t2.onEventTime(time, window, ctx)

  override def canMerge: Boolean = t1.canMerge && t2.canMerge

  override def onMerge(window: W, ctx: Trigger.OnMergeContext): Unit = {
    t1.onMerge(window, ctx)
    t2.onMerge(window, ctx)
  }

  override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
    t1.clear(window, ctx)
    t2.clear(window, ctx)
  }
}
