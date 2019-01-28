package com.deepeshmalviya.learnkafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncProducer {
	
	static Logger logger = LoggerFactory.getLogger(SyncProducer.class);
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create Producer
		logger.info("Creating producer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// create producer record
		logger.info("Creating record");
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("sample_topic", "hello world from sync producer");
		
		// send record to kafka - sync
		logger.info("send is invoked");
		RecordMetadata metadata = producer.send(record).get(); // get will make it blocking call
		System.out.println("Record metadata - offset " + metadata.offset() + " Partition " + metadata.partition());
		
		producer.flush(); // records will not be sent to kafka until this
		producer.close();
		
		
	}
	
}
