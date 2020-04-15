package com.practice.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

        String bootsrapServers = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String ,String> producer = new KafkaProducer<String, String>(properties);

        //ProducerRecord
        ProducerRecord<String , String> record = new ProducerRecord<String , String>("first_topic" , "Hellos");

        //Send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if(e == null)
                {
                    //record sent successfully
                    System.out.println("Received new metadata : \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp" + metadata.timestamp());
                }else{
                    logger.error("Error while producing ",e);
                }
            }
        });

        //flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
