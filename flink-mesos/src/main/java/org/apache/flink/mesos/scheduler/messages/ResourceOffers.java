package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import java.util.List;
import static java.util.Objects.requireNonNull;

/**
 * Message sent by the callback handler to the scheduler actor
 * when resources have been offered to this framework.
 */
public class ResourceOffers {
	private List<Protos.Offer> offers;

	public ResourceOffers(List<Protos.Offer> offers) {
		requireNonNull(offers);
		this.offers = offers;
	}

	public List<Protos.Offer> offers() {
		return offers;
	}

	@Override
	public String toString() {
		return "ResourceOffers{" +
			"offers=" + offers +
			'}';
	}
}
