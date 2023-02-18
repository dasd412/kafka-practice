package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    private final static String TOPIC_NAME = "test";

    private final static String BOOT_STRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class);

        sendMessageWithoutMessageKey(configs);

        sendMessageWithMessageKey(configs);

        sendMessageWithExactPartition(configs);

    }

    private static void sendMessageWithoutMessageKey(Properties configs){
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

        producer.send(record,new ProducerCallback());

        logger.info("{}", record);

        producer.flush();
        producer.close();
    }

    private static void sendMessageWithMessageKey(Properties configs) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "year","2023");

        RecordMetadata metadata=producer.send(record).get();

        logger.info(metadata.toString());

        logger.info("{}", record);

        producer.flush();
        producer.close();
    }

    private static void sendMessageWithExactPartition(Properties configs){
        int partitionNumber=0;

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNumber,"messageKey","2023");

        producer.send(record);

        logger.info("{}", record);

        producer.flush();
        producer.close();
    }
}
