package com.eHanlin.hLog2.worker;

import com.mongodb.BasicDBObject;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

public class RabbitMongoLog extends RabbitMongoWorker {

    public RabbitMongoLog(String configFilePath) throws IOException {
        super(configFilePath);
    }

    public RabbitMongoLog(
        String queueHost, Integer queuePort, String queueName,
        String queueUserName, String queuePassword,
        String dbHost, Integer dbPort, String dbName,
        String dbCollection, String dbCappedCollection, Long dbCappedCollectionSize) throws IOException
    {
        super(queueHost, queuePort, queueName, queueUserName, queuePassword, dbHost, dbPort, dbName, dbCollection, dbCappedCollection, dbCappedCollectionSize);
    }

    @Override
    protected void work(String message, BasicDBObject dbMessage) {
        dbCollection.insert(dbMessage);
        dbCappedCollection.insert(dbMessage);
    }


    public static void main(String[] args) throws IOException {
        RabbitMongoLog rabbitMongoLog = new RabbitMongoLog(args[0]);
        new Thread(new JavaWorkerManager(rabbitMongoLog)).start();
    }

}
