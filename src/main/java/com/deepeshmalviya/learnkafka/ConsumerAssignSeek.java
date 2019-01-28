package com.deepeshmalviya.learnkafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAssignSeek {
	
	final static Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class);
	
	public static void main(String[] args) {
		
		// set consumer related properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sample-topic-group4");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		// assign and seek - used for replay of data or getting data from specific partitions
		TopicPartition partition = new TopicPartition("sample_topic", 0);
		consumer.assign(Arrays.asList(partition));
		
		long offset = 20L;
		consumer.seek(partition, offset);
		
		boolean keepOnReading = true;
		int numOfMessagesToRead = 10;
		int messageCount = 0;
		
		//poll for messages
		while(keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
			
			for(ConsumerRecord<String, String> record: records) {
				messageCount++;
				
				logger.info("Key of record - " + record.key() + ", Value - " + record.value() 
						+ " was read from partition " + record.partition()
						+ " offset " + record.offset());
				
				if(messageCount == numOfMessagesToRead) {
					keepOnReading = false;
					break;
				}
				
			}
			
			
		}
		
		consumer.close();
		
	}
}
