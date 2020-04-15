package com.practice.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootsrapServers = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String ,String> producer = new KafkaProducer<String, String>(properties);

        //ProducerRecord
        ProducerRecord<String , String> record = new ProducerRecord<String , String>("first_topic" , "world");

        //Send data
        producer.send(record);


        //flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
