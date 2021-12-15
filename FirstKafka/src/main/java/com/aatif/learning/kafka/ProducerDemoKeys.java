package com.aatif.learning.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		// create producer record
		for(int i =0 ; i<10;i++) {
			
		
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic",
				"Testing World" + i);
		// send data- this is asynchrouns
		producer.send(producerRecord, new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception e) {
				// executes everytime records is succesffully sent or exception is thrown;
				if (e == null) {
					logger.info("\nRecived new Meta Data + " + metadata.topic() + "\nPartition is " + metadata.partition()
							+ "\nOffset " + metadata.offset() + "\n Timestamp" + metadata.timestamp());
				}else {
					logger.error("Error while Producing" + e);
				}

			}
		});
		}
		/// flush data

		producer.flush();
		producer.close();
		

	}
}
