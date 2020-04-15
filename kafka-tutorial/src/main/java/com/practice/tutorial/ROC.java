package com.practice.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class ROC {
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
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest") ;

        KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumers to topic
        consumer.subscribe(Collections.singletonList(topic));




//        Map<MetricName, ? extends Metric> metrics = consumer.metrics();
//
//            metrics.forEach((metricName,metricValue)->{
//
//                //if(metricName.name().contains("records-consumed-rate"))
//                    System.out.println("------------Metric name: "+metricName.name()+"-----------Metric value: "+metricValue.value());
       //     });


//the position is the latest offset
     //   long offset=consumer.position(actualTopicPartition);


    }
}
