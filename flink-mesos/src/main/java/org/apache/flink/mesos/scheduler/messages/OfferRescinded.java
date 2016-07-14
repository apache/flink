package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

/**
 * Message sent by the callback handler to the scheduler actor
 * when an offer is no longer valid (e.g., the slave was lost or another framework used resources in the offer).
 */
public class OfferRescinded {
	private Protos.OfferID offerId;

	public OfferRescinded(Protos.OfferID offerId) {
		this.offerId = offerId;
	}

	public Protos.OfferID offerId() {
		return offerId;
	}

	@Override
	public String toString() {
		return "OfferRescinded{" +
			"offerId=" + offerId +
			'}';
	}
}
