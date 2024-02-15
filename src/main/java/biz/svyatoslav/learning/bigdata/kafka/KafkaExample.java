package biz.svyatoslav.learning.bigdata.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaExample {
    public static void main(String[] args) {

        // Before you begin, please make sure that Kafka is running.
        // Usually you have to do something like this:
        // cd /home/vmuser/kafka/
        // docker-compose start

        System.out.println("Topic preparation:");
        createTopic();

        System.out.println("Sending messages:");
        produceMessages(20);

        System.out.println("Reading messages (wait a little bit):");
        consumeMessages(); // This will run indefinitely due to the while loop
    }

    private static void createTopic() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(properties)) {
            String topicName = "myNewTopic";

            // Check if the topic already exists
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> existingTopics = listTopics.names().get();
            if (existingTopics.contains(topicName)) {
                System.out.println("Topic " + topicName + " already exists.");
            } else {
                // Create a new topic with 1 partition, replication factor of 1
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic " + topicName + " created successfully.");
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                System.out.println("Topic already exists and couldn't be created.");
            } else {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void produceMessages(int numberOfMessages) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i = 0; i < numberOfMessages; i++) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                ProducerRecord<String, String> record = new ProducerRecord<>("myNewTopic", "key" + (i + 1), "value" + (i + 1));

                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Message " + (i + 1) + " sent to topic " + metadata.topic() + " with offset " + metadata.offset());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void consumeMessages() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton("myNewTopic"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed record with key %s and value %s, from partition %d offset %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }
    }
}
