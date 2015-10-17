/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.test.tugas5pat.tutorial;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author kevhn
 */
public class IRCProducer {
    public static void main(String[] args){
        StringSerializer sskey = new StringSerializer();
        StringSerializer ssval = new StringSerializer();
 
        Map<String,String> props = new HashMap<>();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type","async");
        props.put("request.required.acks", "1");
        props.put("bootstrap.servers", "localhost:9092");
 
        KafkaProducer producer = new KafkaProducer(props,sskey,ssval);
        
        ProducerRecord<String,String> message = new ProducerRecord<>("test","from the body");
        
        producer.send(message,new Callback(){

            @Override
            public void onCompletion(RecordMetadata rm, Exception e) {
                if(e != null)
                    e.printStackTrace();
                System.out.println("The offset of the record we just sent is: " + rm.offset());
            }
            
        });
 
        producer.close();
    }    
}
