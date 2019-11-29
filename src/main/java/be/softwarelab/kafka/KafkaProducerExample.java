package be.softwarelab.kafka;

import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaProducerExample {

    // Kafka constants
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    // comma separate multiple brokers
    // private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    /**
     * Kafka Producer example
     *
     * @param args, the number of messages to produce
     * @throws Exception
     */
    public static void main(String... args) throws Exception {
        Integer messageCount = 5;
        if (args.length != 0) {
            messageCount = Integer.valueOf(args[0]);
        }
        log.info("==== Produce {} messages.", messageCount);
        runProducer(messageCount);
        runProducerAsynchronously(messageCount);
        log.info("==== Kafka Producer ended");
    }

    /**
     * Run a synchronous producer
     *
     * @param sendMessageCount, never null
     * @throws Exception
     */
    public static void runProducer(final int sendMessageCount) throws Exception {
        Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();

        try {
            for (int count = 1; count <= sendMessageCount; count++) {
                // Make the index unique (timestamp)
                Long index = count + time;
                String message = "Synchronous message " + count + " (index:" + index + ")";
                ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, message);
                RecordMetadata metadata = producer.send(record).get();
                long elapsedTime = System.currentTimeMillis() - time;
                log.debug("Record sent [key={} value={}] metadata(partition={}, offset={}) time=%d",
                          record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    /**
     * Asynchronous producer
     *
     * @param sendMessageCount, never null
     * @throws InterruptedException
     */
    public static void runProducerAsynchronously(final int sendMessageCount) throws InterruptedException {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        try {
            for (int count = 1; count <= sendMessageCount; count++) {
                // Make the index unique (timestamp)
                Long index = count + time;
                String message = "Asynchronous message " + count + " (index:" + index + ")";
                ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, message);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        log.debug("Record sent [key={} value={}] metadata(partition={}, offset={}) time=%d",
                                  record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    /**
     * Create a Kafka producer
     *
     * @return kafkaProducer, never null
     */
    @NotNull
    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<Long, String>(props);
    }

}
