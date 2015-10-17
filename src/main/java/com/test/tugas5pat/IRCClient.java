/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.test.tugas5pat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author kevhn
 */
public class IRCClient {
    String nickname = "";
    boolean exitstatus = false;
    KafkaProducer producer;
    HashMap<String,TopicConsumer> channellist;
    
    StringSerializer sskey,ssval;
    static final String alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static Random rand = new Random();
    
    public IRCClient(){
        this.sskey = new StringSerializer();
        this.ssval = new StringSerializer();
        Map<String,String> props = new HashMap<>();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type","async");
        props.put("request.required.acks", "1");
        props.put("bootstrap.servers", "localhost:9092");
 
        producer = new KafkaProducer(props,sskey,ssval);
    }
    
    private void nick(String nick){
        if(nick.equals("")){
            nickname = randomnick();
        }
        else{
            nickname = nick;
        }
        channellist = new HashMap<>();
    }
    
    private String randomnick(){
        StringBuilder randomname = null;
        while (randomname.length() < 10) {
            int index = (int) (rand.nextFloat() * alphanumeric.length());
            randomname.append(alphanumeric.charAt(index));
        }
        return randomname.toString();
    }
    
    private String join(String channel){
        if(channellist.containsKey(channel))
            return "You are already joined the "+channel+" channel";
        TopicConsumer topic = new TopicConsumer(channel, nickname);
        new Thread(topic).start();
        channellist.put(channel,topic);
        return "Successfully joined the "+channel+" channel";
    }
    
    private String leave(String channel){
        if(!channellist.containsKey(channel))
            return "You are not in the "+channel+" channel";
        channellist.get(channel).shutdown();
        channellist.remove(channel);
        return "Successfully left the "+channel+" channel";
    }
    
    private void send(String channel, String message){
        StringBuilder sb = new StringBuilder();
        if(!channel.equals(""))
            sb.append("[").append(channel).append("]");
        sb.append("[").append(nickname).append("]").append(" ").append(message);
        if(channel.equals("")){
            Iterator it = channellist.entrySet().iterator();
            while(it.hasNext()){
                String dest = (String) ((Map.Entry)it.next()).getKey();
                producer.send(new ProducerRecord<>(dest,sb.toString()));
            }
        }
        else{
            producer.send(new ProducerRecord<>(channel,sb.toString()));
        }
    }
    
    private void exit(){
        Iterator it = channellist.entrySet().iterator();
        while(it.hasNext()){
            ((TopicConsumer)((Map.Entry)it.next()).getValue()).shutdown();
        }
        exitstatus = true;
    }
    
    public String process(String command){
        String [] splitted = command.split(" ",2);
        if(splitted[0].equalsIgnoreCase("/nick")){
            try {
                if (splitted.length < 2)
                    nick("");
                else {
                    nick(splitted[1]);
                }
                return "Your nickname is now " + nickname;
            }
            catch (Exception e){
                nickname = "";
                return "Error in assigning nickname: " + e.getMessage();
            }
        }
        else if(splitted[0].equalsIgnoreCase("/join")){
            if(nickname.equals(""))
                return "What's your nickname again?";
            if(splitted.length != 2 )
                return "What's the channel again?";
            else{
                String channelname = splitted[1].toLowerCase();
                try {
                    String retval = join(channelname);
                    return retval;
                }
                catch (Exception e){
                    return "Error in joining channel: " +e.getMessage();
                }
            }
        }
        else if(splitted[0].equalsIgnoreCase("/leave")){
            if(nickname.equals(""))
                return "What's your nickname again?";
            if(splitted.length != 2 )
                return "What's the channel again?";
            else{
                String channelname = splitted[1].toLowerCase();
                try {
                    String retval = leave(channelname);
                    return retval;
                }
                catch (Exception e){
                    return "Error in leaving channel: " + e.getMessage();
                }
            }
        }
        else if(splitted[0].equalsIgnoreCase("/exit")){
            try {
                exit();
                return "Exiting...";
            }
            catch (Exception e){
                return "Problem in exiting.";
            }
        }
        else if(splitted[0].startsWith("@")){
            if(nickname.equals(""))
                return "What's your nickname again?";
            if (splitted.length < 2){
                return "What's the message again?";
            } else {
                String channelname = splitted[0].substring(1);
                if (!channellist.containsKey(channelname))
                    return "You haven't joined the " + channelname + " channel yet!";
                try {
                    send(channelname, splitted[1]);
                    return "Message sent to "+channelname;
                } catch (Exception e) {
                    return "Error in sending message: " + e.getMessage();
                }
            }

        }
        else{
            if(nickname.equals(""))
                return "What's your nickname again?";
            if (splitted.length < 1){
                return "What's the message again?";
            } else {
                try {
                    if(splitted.length > 1)
                        send("", splitted[0]+" "+splitted[1]);
                    else
                        send("", splitted[0]);
                    return "Message sent";
                } catch (Exception e) {
                    return "Error in sending message: " + e.getMessage();
                }
            }
        }
    }
    
    public static void main(String[] argv){
        IRCClient client = null;
        try{
            client = new IRCClient();
        }
        catch(Exception e){
            System.exit(1);
        }
        System.out.println("Welcome to very simple IRC Client!");
        System.out.println("Usages:");
        System.out.println("/NICK <nickname> to select nickname");
        System.out.println("/JOIN <channelname> to join channel");
        System.out.println("/LEAVE <channelname> to leave channel");
        System.out.println("@<channelname> <message> to send message within selected channel");
        System.out.println("<message> to send with all");
        System.out.println("/EXIT to exit");
        Scanner cin = new Scanner(System.in);
        String str;
        do {
            str = cin.nextLine();
            System.out.println(client.process(str));
        }
        while(!client.exitstatus);
        
    }
}
