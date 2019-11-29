package be.softwarelab.kafka;

import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaProducer {

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
        return new org.apache.kafka.clients.producer.KafkaProducer<Long, String>(props);
    }

}
