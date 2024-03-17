package jsh.kafkaproducer.producer;

import jsh.kafkaproducer.partitioner.CustomPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class SimpleProducer {

    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);


        //ProducerRecord<String, String> record = getBasicRecord(TOPIC_NAME, "testMessage");

        //ProducerRecord<String, String> record = getMsgKeyRecord(TOPIC_NAME, "myKeyMsg", "myKey");

        //ProducerRecord<String, String> record = getPartitionNoSettedRecord(TOPIC_NAME, 0, "partitionMsg", "partition");

        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        ProducerRecord<String, String> record = getMsgKeyRecord(TOPIC_NAME, "PangyoMsg", "Pangyo");
        log.info("{}", record);

        try {
            RecordMetadata metadata = producer.send(record).get();
            log.info(metadata.toString());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static ProducerRecord<String, String> getBasicRecord(String topicName, String messageValue) {
        return new ProducerRecord<>(topicName, messageValue);
    }

    private static ProducerRecord<String, String> getMsgKeyRecord(String topicName, String messageValue, String key) {
        return new ProducerRecord<>(topicName, messageValue, key);
    }

    private static ProducerRecord<String, String> getPartitionNoSettedRecord(String topicName, int partitionNo, String messageValue, String key) {
        return new ProducerRecord<>(topicName, partitionNo, messageValue, key);
    }
}
