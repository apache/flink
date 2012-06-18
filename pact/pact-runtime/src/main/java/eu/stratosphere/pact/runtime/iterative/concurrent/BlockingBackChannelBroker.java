package eu.stratosphere.pact.runtime.iterative.concurrent;

import eu.stratosphere.pact.runtime.iterative.Identifier;

/** Singleton class for the threadsafe handover of {@link BlockingBackChannel}s from iteration heads to iteration tails */
public class BlockingBackChannelBroker extends Broker<Identifier, BlockingBackChannel> {

  private static final BlockingBackChannelBroker INSTANCE = new BlockingBackChannelBroker();

  private BlockingBackChannelBroker() {}

  /** retriefve singleton instance */
  public static Broker<Identifier, BlockingBackChannel> instance() {
    return INSTANCE;
  }
}
