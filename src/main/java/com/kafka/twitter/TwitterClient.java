package com.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterClient {

    static final String consumerKey = "";
    static final String consumerKeySecret = "";
    static final String accessToken = "";
    static final String accessTokenSecret = "";

    List<String> terms = Lists.newArrayList("kafka");

    public static void main(String[] args) throws InterruptedException {
        new TwitterClient().run();
    }

    public void run(){

        Logger logger = LoggerFactory.getLogger(TwitterClient.class);

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);

        //Creating Twitter Client
        Client client = this.createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        //Creating a KafkaProducer
        KafkaProducer<String, String> producer = this.createKafkaProducer();

        //Adding a ShutDown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));


        //Running Loop for msgs
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Error while fetching Message", e);
                client.stop();
            }

            if(msg != null){
                logger.info("\nGenerated Msg: " + msg);
                producer.send(new ProducerRecord<String, String>("twitter-tweets", msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Error generated while producing tweets", e);
                        }
                    }
                });
            }

        }
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();


        endpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication auth = new OAuth1(consumerKey, consumerKeySecret, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client = builder.build();
        return client;
    }

    public KafkaProducer<String, String> createKafkaProducer(){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
