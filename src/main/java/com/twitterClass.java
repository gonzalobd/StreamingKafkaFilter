
package com;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class twitterClass {
    public static String KAFKA_HOST = "localhost:9092";
    public static String TOPIC_IN = "twitter";
    public static String TOPIC_OUT="twitter_filtered";
    private static final AtomicBoolean closed = new AtomicBoolean(false);

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                System.out.println("Shutting down");
                closed.set(true);
            }
        });

        Properties props_cons = new Properties();
        props_cons.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props_cons.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props_cons.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props_cons.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props_cons.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props_cons.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "serializers.JsonDeserializer");

        KafkaConsumer<String, Map<String,Object>> consumer = new KafkaConsumer<>(props_cons);
        consumer.subscribe(Collections.singletonList(TOPIC_IN));

        Properties props_proc = new Properties();
        props_proc.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props_proc.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props_proc.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "serializers.JsonSerializer");
        props_proc.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "serializers.SimplePartitioner");
        Producer<String, Map<String, Object>> producer = new KafkaProducer<>(props_proc);

        while (!closed.get()) {
            ConsumerRecords<String,Map<String,Object>> records = consumer.poll(100);
            for (ConsumerRecord<String, Map<String,Object>> record : records) {
                System.out.println("Tweet readed and filtered in topic: "+record.topic()+record.value().get("created_at"));
                System.out.println("readed"+record.value());
                Map<String, Object> tweetFilt = new HashMap<>();
                String created_at=record.value().get("created_at").toString();
                tweetFilt.put("created_at",created_at);
                if(record.value().get("coordinates")!=null){String coordinates=record.value().get("coordinates").toString();
                tweetFilt.put("coordinates",coordinates);}
                String lang=record.value().get("lang").toString();
                tweetFilt.put("lang",lang);
                if (record.value().get("place")!=null){String place=record.value().get("place").toString();
                tweetFilt.put("place",place);}
                Integer retweet_count=(Integer)record.value().get("retweet_count");
                tweetFilt.put("retweet_count",retweet_count);
                Integer favorite_count=(Integer)record.value().get("favorite_count");
                tweetFilt.put("favorite_count",favorite_count);
                String text=record.value().get("text").toString();
                tweetFilt.put("text",text);
                producer.send(new ProducerRecord<>(TOPIC_OUT, "Filtered_tweet", tweetFilt));


            }
        }
        consumer.close();
    }
}
