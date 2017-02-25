
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
        Map<String, Object> tweetFilt = new HashMap<>();
        while (!closed.get()) {
            ConsumerRecords<String,Map<String,Object>> records = consumer.poll(100);
            for (ConsumerRecord<String, Map<String,Object>> record : records) {
                tweetFilt.clear();
                System.out.println("Tweet readed from topic: "+record.topic());
                if(record.value().get("created_at")!=null){
                    String created_at=record.value().get("created_at").toString();
                    tweetFilt.put("created_at",created_at);}
                    else{tweetFilt.put("created_at","notAvailable");}
                if(record.value().get("coordinates")!=null){
                    String coordinates=record.value().get("coordinates").toString();
                    tweetFilt.put("coordinates",coordinates);}
                    else{tweetFilt.put("coordinates","notAvailable");}
                if(record.value().get("lang")!=null){
                    String lang=record.value().get("lang").toString();
                    tweetFilt.put("lang",lang);}
                    else{tweetFilt.put("lang","notAvailable");}
                if (((Map)record.value().get("user")).get("time_zone")!=null){
                    String place=((Map)record.value().get("user")).get("time_zone").toString();
                    tweetFilt.put("time_zone",place);}
                    else{tweetFilt.put("time_zone","notAvailable");}
                if (record.value().get("retweet_count")!=null){
                    Integer retweet_count=(Integer)record.value().get("retweet_count");
                    tweetFilt.put("retweet_count",retweet_count);}
                    else{tweetFilt.put("retweet_count","notAvailable");}
                if (record.value().get("favorite_count")!=null){
                    Integer favorite_count=(Integer)record.value().get("favorite_count");
                    tweetFilt.put("favorite_count",favorite_count);}
                    else{tweetFilt.put("favorite_count","notAvailable");}
                if (record.value().get("text")!=null){
                    String text=record.value().get("text").toString();
                    tweetFilt.put("text",text);}
                    else{tweetFilt.put("text","notAvailable");}
                producer.send(new ProducerRecord<>(TOPIC_OUT, "Filtered_tweet", tweetFilt));
                System.out.println("Tweet sent: "+tweetFilt.get("text"));

            }
        }
        consumer.close();
    }
}
