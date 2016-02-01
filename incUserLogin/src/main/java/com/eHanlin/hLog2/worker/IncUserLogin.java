package com.eHanlin.hLog2.worker;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

public class IncUserLogin extends RabbitMongoWorker {

    private long minute = 1000 * 60;

    public IncUserLogin(String configFilePath) throws IOException {
        super(configFilePath);
    }

    public IncUserLogin(
        String queueHost, Integer queuePort, String queueName,
        String queueUserName, String queuePassword,
        String dbHost, Integer dbPort, String dbName,
        String dbCollection, String dbCappedCollection, Long dbCappedCollectionSize) throws IOException
    {
        super(queueHost, queuePort, queueName, queueUserName, queuePassword, dbHost, dbPort, dbName, dbCollection, dbCappedCollection, dbCappedCollectionSize);
    }

    @Override
    protected void work(String message, BasicDBObject dbMessage) {
        long date = Long.parseLong(dbMessage.get("createDate").toString());
        date = date - (date % minute);
        DBObject dbFind = BasicDBObjectBuilder.start().add("date", date).add("action", "login").get();
        DBObject dbUpdate = new BasicDBObject("$inc", new BasicDBObject("count", 1L));
        dbCollection.update(dbFind, dbUpdate, true, false);
        dbCappedCollection.update(dbFind, dbUpdate, true, false);
    }


    public static void main(String[] args) throws IOException {
        IncUserLogin incUserLogin = new IncUserLogin(args[0]);
        new Thread(new JavaWorkerManager(incUserLogin)).start();
    }
}
