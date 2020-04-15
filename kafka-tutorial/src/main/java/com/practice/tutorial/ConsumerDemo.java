package com.practice.tutorial;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String BootstrapServers="127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-third-application";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);


        //create consumer
        KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumers to topic
        consumer.subscribe(Collections.singletonList(topic));


//

        //poll for the new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Value: " + record.value() + " , Partition: " + record.partition() + " ," + "Offset: " + record.offset());

                //Calculating Lag
                Set<TopicPartition> partitions = new HashSet<TopicPartition>();
                TopicPartition actualTopicPartition = new TopicPartition(record.topic(), record.partition());
                partitions.add(actualTopicPartition);
                Long actualEndOffset = consumer.endOffsets(partitions).get(actualTopicPartition);
                long actualPosition = consumer.position(actualTopicPartition);
                System.out.println(String.format("LAG : %s   (actualEndOffset:%s; actualPosition=%s)", actualEndOffset - actualPosition, actualEndOffset, actualPosition));

               //Calculating Rate of Consumption
            Map<MetricName, ? extends Metric> metrics = consumer.metrics();

            metrics.forEach((metricName,metricValue)->{

                if(metricName.name().contains("records-consumed-rate"))
                {
                    System.out.println("----Metric name: "+metricName.name()+"---Metric value: "+metricValue.value());

                    System.out.println("Time to cover the Lag : "+ (actualEndOffset - actualPosition)/metricValue.value()+" seconds");
                }
            });


        }
        }


    }


}
