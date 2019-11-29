package be.softwarelab.kafka;

import lombok.extern.slf4j.Slf4j;

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
}
