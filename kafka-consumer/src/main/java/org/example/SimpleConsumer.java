package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class SimpleConsumer {

    private static final Logger logger= LoggerFactory.getLogger(SimpleConsumer.class);

    private final static String TOPIC_NAME="test";

    private final static String BOOTSTRAP_SERVERS="localhost:9092";

    private final static String GROUP_ID="test_group";

    private static KafkaConsumer<String,String> consumer;

    private static Map<TopicPartition,OffsetAndMetadata>currentOffsets=new HashMap<>();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs=new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        consumer= new KafkaConsumer<>(configs);

        consumer.subscribe(Arrays.asList(TOPIC_NAME),new ReBalanceListener());

        try{
            while (true){
                ConsumerRecords<String,String>records=consumer.poll(Duration.ofSeconds(1));

                for(ConsumerRecord<String,String>record:records){
                    logger.info("{}",record);
                    currentOffsets.put(
                            new TopicPartition(record.topic(),record.partition()),
                            new OffsetAndMetadata(record.offset()+1,null));
                    consumer.commitSync(currentOffsets);
                }
            }
        }catch (WakeupException e){
            logger.warn("Wakeup consumer");
            // 리소스 종료 로직 넣으면 됨.
        }finally {
            consumer.close();
        }
    }

    static class ShutdownThread extends Thread{
        public void run(){
            logger.info("shutdown hook");
            consumer.wakeup();
        }
    }

    private static class ReBalanceListener implements ConsumerRebalanceListener{

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            logger.warn("Partitions are assigned");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            logger.warn("partitions are revoked");
            consumer.commitSync(currentOffsets);
        }
    }
}