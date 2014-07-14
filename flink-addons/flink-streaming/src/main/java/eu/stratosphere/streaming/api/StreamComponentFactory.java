package eu.stratosphere.streaming.api;

import java.util.List;
import java.util.Map;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public final class StreamComponentFactory {
	//TODO: put setConfigInputs here

	public static void setAckListener(Map<String, StreamRecord> recordBuffer,
			String sourceInstanceID, List<RecordWriter<Record>> outputs) {
		EventListener eventListener = new AckEventListener(sourceInstanceID,
				recordBuffer);
		for (RecordWriter<Record> output : outputs) {
			// TODO: separate outputs
			output.subscribeToEvent(eventListener, AckEvent.class);
		}
	}

	public static void setPartitioner(Configuration taskConfiguration,
			int nrOutput, List<ChannelSelector<Record>> partitioners) {
		Class<? extends ChannelSelector<Record>> partitioner = taskConfiguration
				.getClass("partitionerClass_" + nrOutput, DefaultPartitioner.class,
						ChannelSelector.class);

		try {
			if (partitioner.equals(FieldsPartitioner.class)) {
				int keyPosition = taskConfiguration.getInteger("partitionerIntParam_"
						+ nrOutput, 1);
				Class<? extends Key> keyClass = taskConfiguration.getClass(
						"partitionerClassParam_" + nrOutput, StringValue.class, Key.class);

				partitioners.add(partitioner.getConstructor(int.class, Class.class)
						.newInstance(keyPosition, keyClass));

			} else {
				partitioners.add(partitioner.newInstance());
			}
		} catch (Exception e) {
			System.out.println("partitioner error" + " " + "partitioner_" + nrOutput);
			System.out.println(e);
		}
	}
}