/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.test.tugas5pat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 *
 * @author kevhn
 */
public class TopicConsumer implements Runnable{
    
    private String topic;
    private String nick;
    private ConsumerConnector consumer;
    
    public TopicConsumer(String a_topic, String a_nick){
        topic = a_topic;
        nick = a_nick;
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_nick));
    }
    
    private ConsumerConfig createConsumerConfig(String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", a_groupId);
 
        return new ConsumerConfig(props);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        
        for(KafkaStream stream : streams){
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext())
                System.out.println(new String(it.next().message()));
        }
    }
    
    public void shutdown(){
        consumer.shutdown();
    }
    
}
