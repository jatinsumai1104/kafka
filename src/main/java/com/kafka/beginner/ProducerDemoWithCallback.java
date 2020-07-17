package com.kafka.beginner;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        //Logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 10; i++) {
            //Create Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "Hello World" + i + "!");

            //Send Data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //Data successfully inserted
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        //Error Occurred
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        //Closing producer and flushing data
        producer.flush();
        producer.close();
    }
}
