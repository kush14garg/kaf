package com.practice.tutorial;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class CalculatingLag {
    public static void main(String[] args) {
        String BootstrapServers="127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-third-application";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);

        KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumers to topic
       // consumer.subscribe(Collections.singletonList(topic));

        TopicPartition tp = new TopicPartition(topic, 0);
        Collections.singletonList(tp);
        consumer.assign(Collections.singletonList(tp));
        consumer.seekToEnd(Collections.singletonList(tp));
        long Endoffset= consumer.position(tp);
        System.out.println("END offset : "+Endoffset);



//        Map<MetricName, ? extends Metric> metrics = consumer.metrics();
//
//        metrics.forEach((metricName,metricValue)->{
//
//            if(metricName.name().contains("records-lag"))
//                System.out.println("------------Metric name: "+metricName.name()+"-----------Metric value: "+metricValue.value());
//        });
    }
}
