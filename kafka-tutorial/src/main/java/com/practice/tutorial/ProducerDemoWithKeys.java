package com.practice.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

        String bootsrapServers = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String ,String> producer = new KafkaProducer<String, String>(properties);
        for(int i =0;i<100000;i++) {
            String topic ="first_topic";
            String value = "hello "+Integer.toString(i);
            String key= "id_"+Integer.toString(i);

            //ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key , value);
            System.out.println("Key: "+key);
            //Send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        //record sent successfully
                        System.out.println("Received new metadata : \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp" + metadata.timestamp());
                    } else {
                        logger.error("Error while producing ", e);
                    }
                }
            }).get();

        }


//        Map<String, String> metricTags = new LinkedHashMap<String, String>();
//        metricTags.put("client-id", "producer-1");
//        metricTags.put("topic", "first_topic");
////
//        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
//        Metrics metrics = new Metrics(metricConfig); // this is the global repository of metrics and sensors
////
//        Sensor sensor = metrics.sensor("message-sizes");
////
//        MetricName metricName = metrics.metricName("message-size-avg", "producer-metrics", "average message size");
//        sensor.add(metricName, new Avg());
//
//        for (Map.Entry<MetricName, ? extends Metric> entry : producer.metrics().entrySet()) {
//            System.out.println(entry.getKey().name() + " : " + entry.getValue().value());
//        }
//
//        metricName = metrics.metricName("message-size-max", "producer-metrics");
//        sensor.add(metricName, new Max());
//
//        metricName = metrics.metricName("message-size-min", "producer-metrics", "message minimum size", "client-id", "my-client", "topic", "my-topic");
//        sensor.add(metricName, new Min());
//
//        // as messages are sent we record the sizes
//        sensor.record(messageSize);

        //flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
