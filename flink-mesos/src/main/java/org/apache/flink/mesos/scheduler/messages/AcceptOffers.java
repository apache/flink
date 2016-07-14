package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import java.util.Collection;

/**
 * Local message sent by the launch coordinator to the scheduler to accept offers.
 */
public class AcceptOffers {

	private String hostname;
	private Collection<Protos.OfferID> offerIds;
	private Collection<Protos.Offer.Operation> operations;
	private Protos.Filters filters;

	public AcceptOffers(String hostname, Collection<Protos.OfferID> offerIds, Collection<Protos.Offer.Operation> operations) {
		this.hostname = hostname;
		this.offerIds = offerIds;
		this.operations = operations;
		this.filters = Protos.Filters.newBuilder().build();
	}

	public AcceptOffers(String hostname, Collection<Protos.OfferID> offerIds, Collection<Protos.Offer.Operation> operations, Protos.Filters filters) {
		this.hostname = hostname;
		this.offerIds = offerIds;
		this.operations = operations;
		this.filters = filters;
	}

	public String hostname() {
		return hostname;
	}

	public Collection<Protos.OfferID> offerIds() {
		return offerIds;
	}

	public Collection<Protos.Offer.Operation> operations() {
		return operations;
	}

	public Protos.Filters filters() {
		return filters;
	}

	@Override
	public String toString() {
		return "AcceptOffers{" +
			"hostname='" + hostname + '\'' +
			", offerIds=" + offerIds +
			", operations=" + operations +
			", filters=" + filters +
			'}';
	}
}
