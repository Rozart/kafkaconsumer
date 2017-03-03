package nl.oramon.kafkaconsumer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Consumer {

    private static final String PROPERTIES_PATH = "src/main/resources/consumer.properties";
    private static final String GROUP_ID_PROPERTY_KEY = "group.id";

    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("Usage: consumer <topic> <groupname>");
            return;
        }

        String topic = args[0];
        String groupId = args[1];

        Properties props = new Properties();
        props.load(new FileInputStream(PROPERTIES_PATH));
        props.setProperty(GROUP_ID_PROPERTY_KEY, groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));

        System.out.println("Subscribed to topic " + topic);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(0);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}