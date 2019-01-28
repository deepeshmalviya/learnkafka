package com.deepeshmalviya.learnkafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class AsyncProducer {
	
	public static void main(String[] args) {
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// create producer record
		for(int i=0; i<10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("sample_topic", "hello world" + i);
			
			// send record to kafka - async with callback
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						System.out.println("Record metadata - offset " + metadata.offset() + " Partition " + metadata.partition());
					} else {
						exception.printStackTrace();
					}
					
				}
			});
		}
		
		producer.flush();
		producer.close();
		
		
	}
	
}
