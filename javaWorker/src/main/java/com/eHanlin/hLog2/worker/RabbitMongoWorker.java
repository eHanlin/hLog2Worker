package com.eHanlin.hLog2.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * RabbitMQ autoAck is off, need channel.basicAck after run.
 */
public abstract class RabbitMongoWorker implements Runnable {

    protected Channel queueChannel = null;
    protected QueueingConsumer queueConsumer = null;

    protected DBCollection dbCollection = null;
    protected DBCollection dbCappedCollection = null;

    protected ObjectMapper objectMapper = new ObjectMapper();

    public RabbitMongoWorker(String configFilePath) throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(configFilePath));

        String queueHost = props.getProperty("rabbitMQ.host");
        Integer queuePort = Integer.parseInt(props.getProperty("rabbitMQ.port"));
        String queueName = props.getProperty("rabbitMQ.queue");
        String queueUserName = props.getProperty("rabbitMQ.userName");
        String queuePassword = props.getProperty("rabbitMQ.password");

        String dbHost = props.getProperty("mongoDB.host");
        Integer dbPort = Integer.parseInt(props.getProperty("mongoDB.port"));
        String dbName = props.getProperty("mongoDB.db");
        String dbCollection = props.getProperty("mongoDB.collection");
        String dbCappedCollection = props.getProperty("mongoDB.cappedCollection");
        Long dbCappedCollectionSize = Long.parseLong(props.getProperty("mongoDB.cappedCollection.size"));

        setupRabbitMQ(queueHost, queuePort, queueName, queueUserName, queuePassword);
        setupMongoDB(dbHost, dbPort, dbName, dbCollection, dbCappedCollection, dbCappedCollectionSize);
    }

    public RabbitMongoWorker(
        String queueHost, Integer queuePort, String queueName,
        String queueUserName, String queuePassword,
        String dbHost, Integer dbPort, String dbName,
        String dbCollection, String dbCappedCollection, Long dbCappedCollectionSize) throws IOException
    {
        setupRabbitMQ(queueHost, queuePort, queueName, queueUserName, queuePassword);
        setupMongoDB(dbHost, dbPort, dbName, dbCollection, dbCappedCollection, dbCappedCollectionSize);
    }

    protected void setupRabbitMQ(
        String queueHost, Integer queuePort, String queueName,
        String queueUserName, String queuePassword) throws IOException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(queueHost);
        factory.setPort(queuePort);
        factory.setUsername(queueUserName);
        factory.setPassword(queuePassword);
        queueChannel = factory.newConnection().createChannel();
        queueChannel.queueDeclare(queueName, true, false, false, null);
        queueConsumer = new QueueingConsumer(queueChannel);
        queueChannel.basicConsume(queueName, false, queueConsumer);
    }

    protected void setupMongoDB(
        String dbHost, Integer dbPort, String dbName,
        String dbCollection, String dbCappedCollection, Long dbCappedCollectionSize) throws IOException
    {
        MongoClient mongoClient = new MongoClient(dbHost , dbPort);
        DB db = mongoClient.getDB(dbName);
        this.dbCollection = db.getCollection(dbCollection);

        if(db.collectionExists(dbCappedCollection)){
            this.dbCappedCollection = db.getCollection(dbCappedCollection);
            if(!this.dbCappedCollection.isCapped()){
                DBObject command = BasicDBObjectBuilder.start().add("convertToCapped", dbCollection).add("size", dbCappedCollectionSize).get();
                db.command(command);
                this.dbCappedCollection = db.getCollection(dbCappedCollection);
            }
        }else{
            DBObject options = BasicDBObjectBuilder.start().add("capped", true).add("size", dbCappedCollectionSize).get();
            this.dbCappedCollection = db.createCollection(dbCappedCollection, options);
        }
    }

    @Override
    public void run() {
        try {
            QueueingConsumer.Delivery delivery = queueConsumer.nextDelivery(1000L);
            if(delivery != null){
                String message = new String(delivery.getBody());
                BasicDBObject dbMessage = objectMapper.readValue(message, BasicDBObject.class);
                work(message, dbMessage);
                queueChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }

    }

    protected abstract void work(String message, BasicDBObject dbMessage);
}
