package eu.stratosphere.streaming.kafka;

import java.util.*;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.SinkFunction;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaSink extends SinkFunction<Tuple1<String>>{
	private static final long serialVersionUID = 1L;
	
	static kafka.javaapi.producer.Producer<Integer, String> producer;
	static Properties props;
	private String topicId;
	private String brokerAddr;
	
	public KafkaSink(String topicId, String brokerAddr){
		this.topicId=topicId;
		this.brokerAddr=brokerAddr;
		
		
	}
	public void initialize() {
		props = new Properties();
		 
		props.put("metadata.broker.list", brokerAddr);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<Integer, String>(config);
	}
	
	@Override
	public void invoke(Tuple1<String> tuple) {
		initialize();
		
		KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topicId, tuple.f0);
        producer.send(data);
        producer.close();
        
	}

}
