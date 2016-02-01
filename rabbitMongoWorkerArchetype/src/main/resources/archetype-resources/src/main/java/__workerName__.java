#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package};

import com.mongodb.BasicDBObject;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

public class ${workerName} extends RabbitMongoWorker {

    public ${workerName}(String configFilePath) throws IOException {
        super(configFilePath);
    }

    public ${workerName}(
        String queueHost, Integer queuePort, String queueName,
        String queueUserName, String queuePassword,
        String dbHost, Integer dbPort, String dbName,
        String dbCollection, String dbCappedCollection, Long dbCappedCollectionSize) throws IOException
    {
        super(queueHost, queuePort, queueName, queueUserName, queuePassword, dbHost, dbPort, dbName, dbCollection, dbCappedCollection, dbCappedCollectionSize);
    }

    @Override
    protected void work(String message, BasicDBObject dbMessage) {
        //TODO insert work code
    }


    public static void main(String[] args) throws IOException {
        ${workerName} ${artifactId} = new ${workerName}(args[0]);
        new Thread(new JavaWorkerManager(${artifactId})).start();
    }
}
