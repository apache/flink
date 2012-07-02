package eu.stratosphere.pact.runtime.iterative.concurrent;

public class SuperstepBarrierBroker extends Broker<SuperstepBarrier> {

  /** single instance */
  private static final SuperstepBarrierBroker INSTANCE = new SuperstepBarrierBroker();

  private SuperstepBarrierBroker() {}

  /** retrieve singleton instance */
  public static Broker<SuperstepBarrier> instance() {
    return INSTANCE;
  }

}
