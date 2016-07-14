package org.apache.flink.mesos.util;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import scala.Option;

import java.util.Map;

public class MesosConfiguration {

	private String masterUrl;

	private Protos.FrameworkInfo.Builder frameworkInfo;

	private Option<Protos.Credential.Builder> credential = Option.empty();

	public MesosConfiguration(
		String masterUrl,
		Protos.FrameworkInfo.Builder frameworkInfo,
		Option<Protos.Credential.Builder> credential) {

		this.masterUrl = masterUrl;
		this.frameworkInfo = frameworkInfo;
		this.credential = credential;
	}

	public String masterUrl() {
		return masterUrl;
	}

	public Protos.FrameworkInfo.Builder frameworkInfo() {
		return frameworkInfo;
	}

	public Option<Protos.Credential.Builder> credential() {
		return credential;
	}

	/**
	 * Revise the configuration with updated framework info.
     */
	public MesosConfiguration withFrameworkInfo(Protos.FrameworkInfo.Builder frameworkInfo) {
		return new MesosConfiguration(masterUrl, frameworkInfo, credential);
	}

	/**
	 * Create the Mesos scheduler driver based on this configuration.
	 * @param scheduler the scheduler to use.
	 * @param implicitAcknowledgements whether to configure the driver for implicit implicit acknowledgements.
     * @return a scheduler driver.
     */
	public SchedulerDriver createDriver(Scheduler scheduler, boolean implicitAcknowledgements) {
		MesosSchedulerDriver schedulerDriver;
		if(this.credential().isDefined()) {
			schedulerDriver =
				new MesosSchedulerDriver(scheduler, frameworkInfo.build(), this.masterUrl(), false,
					this.credential().get().build());
		}
		else {
			schedulerDriver =
				new MesosSchedulerDriver(scheduler, frameworkInfo.build(), this.masterUrl(), false);
		}
		return schedulerDriver;
	}

	@Override
	public String toString() {
		return "MesosConfiguration{" +
			"masterUrl='" + masterUrl + '\'' +
			", frameworkInfo=" + frameworkInfo +
			", credential=" + (credential.isDefined() ? "(not shown)" : "(none)") +
			'}';
	}

	/**
	 * A utility method to log relevant Mesos connection info
     */
	public static void logMesosConfig(Logger log, MesosConfiguration config) {

		Map<String,String> env = System.getenv();
		Protos.FrameworkInfo.Builder info = config.frameworkInfo();

		log.info("--------------------------------------------------------------------------------");
		log.info(" Mesos Info:");
		log.info("    Master URL: {}", config.masterUrl());

		log.info(" Framework Info:");
		log.info("    ID: {}", info.hasId() ? info.getId().getValue() : "(none)");
		log.info("    Name: {}", info.hasName() ? info.getName() : "(none)");
		log.info("    Failover Timeout (secs): {}", info.getFailoverTimeout());
		log.info("    Role: {}", info.hasRole() ? info.getRole() : "(none)");
		log.info("    Principal: {}", info.hasPrincipal() ? info.getPrincipal() : "(none)");
		log.info("    Host: {}", info.hasHostname() ? info.getHostname() : "(none)");
		if(env.containsKey("LIBPROCESS_IP")) {
			log.info("    LIBPROCESS_IP: {}", env.get("LIBPROCESS_IP"));
		}
		if(env.containsKey("LIBPROCESS_PORT")) {
			log.info("    LIBPROCESS_PORT: {}", env.get("LIBPROCESS_PORT"));
		}
		log.info("    Web UI: {}", info.hasWebuiUrl() ? info.getWebuiUrl() : "(none)");

		log.info("--------------------------------------------------------------------------------");

	}
}
