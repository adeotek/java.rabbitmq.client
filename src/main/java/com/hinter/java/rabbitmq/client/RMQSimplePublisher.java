package com.hinter.java.rabbitmq.client;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RMQSimplePublisher {
    private static final Logger appLogger = LogManager.getLogger(RMQSimplePublisher.class);

    protected ConnectionFactory pConnectionFactory;
    protected Connection mConnection = null;
    protected Channel mChannel = null;
    protected String mExchangeName;
    protected String mExchangeType;
    protected String mQueueName;
    protected String mRoutingKey;
    protected int mRetries;

    protected static ConnectionFactory getConnectionFactory(String connectionString, Boolean automaticRecovery) throws Exception {
        appLogger.info("Triggered: -");
        if (Helpers.isStringEmptyOrNull(connectionString)) {
            throw new Exception("Invalid connection string!");
        }
        ConnectionFactory connectionFactory = new ConnectionFactory();
        if(automaticRecovery!=null) {
            connectionFactory.setAutomaticRecoveryEnabled(automaticRecovery);
        }
        connectionFactory.setUri(connectionString);
        return connectionFactory;
    }//getConnectionFactory

    protected static ConnectionFactory getConnectionFactory(String server, int port, String username, String password, Boolean automaticRecovery, int timeout) throws Exception {
        appLogger.info("Triggered: -");
        if (Helpers.isStringEmptyOrNull(server)) {
            throw new Exception("Invalid connection parameters: server");
        }
        if (port<=0) {
            throw new Exception("Invalid connection parameters: port");
        }
        if (!Helpers.isPortOpen(server, port, timeout)) {
            throw new Exception("RabbitMQ server [" + server + ":" + port + "] is unreachable!");
        }
        ConnectionFactory connectionFactory = new ConnectionFactory();
        if(automaticRecovery!=null) {
            connectionFactory.setAutomaticRecoveryEnabled(automaticRecovery);
        }
        if (!Helpers.isStringEmptyOrNull(username)) {
            connectionFactory.setUsername(username);
            connectionFactory.setPassword((Helpers.isStringEmptyOrNull(password) ? "" : password));
        }
        connectionFactory.setHost(server);
        connectionFactory.setPort(port);
        return connectionFactory;
    }//getConnectionFactory

    protected static boolean doPublishOneMessage(final ConnectionFactory connectionFactory, String message, String exchangeName, String exchangeType, String queueName, String routingKey, boolean autoACK, int retries) {
        appLogger.info("Triggered: -");
        boolean result = false;
        Connection connection = null;
        Channel channel = null;
        while(retries>0) {
            try {
                connection = connectionFactory.newConnection();
                channel = connection.createChannel();
                channel.exchangeDeclare(exchangeName, exchangeType, true);
                channel.confirmSelect();
                channel.queueDeclare(queueName, true, false, false, null);
                appLogger.info("Queue: " + queueName);
                channel.queueBind(queueName, exchangeName, routingKey);
                channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
                appLogger.info("Message published [s]");
                if (!autoACK) {
                    channel.waitForConfirmsOrDie();//Confirms (aka Publisher Acknowledgements) throws IOException
                }
                result = true;
            } catch (IOException ioe) {
                appLogger.error("Message ACK error: " + ioe.getMessage());
                appLogger.debug("Message [f]: " + message);
            } catch (Exception e) {
                appLogger.error("Message error: " + e.getMessage());
                appLogger.debug("Message [f]: " + message);
            } finally {
                if(channel!=null) {
                    try {
                        channel.close();
                    } catch (IOException | TimeoutException e) {
                        appLogger.error("Unable to close Channel: " + e.getMessage());
                    }
                }
                if(connection!=null) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        appLogger.error("Unable to close Connection: " + e.getMessage());
                    }
                }
            }
            retries--;
        }
        return result;
    }//PublishOneMessage

    public static boolean PublishOneMessage(String message, String exchangeName, String exchangeType, String queueName, String routingKey, boolean autoACK, String connectionString, Boolean automaticRecovery, int retries) {
        appLogger.info("Triggered: -");
        ConnectionFactory connectionFactory;
        try {
            connectionFactory = getConnectionFactory(connectionString, automaticRecovery);
        } catch(Exception e) {
            appLogger.error("PublishOneMessage Exception: " + e.getMessage());
            return false;
        }
        return doPublishOneMessage(connectionFactory, message, exchangeName, exchangeType, queueName, routingKey, autoACK, retries);
    }//PublishOneMessage

    public static boolean PublishOneMessage(String message, String exchangeName, String exchangeType, String queueName, String routingKey, boolean autoACK, String server, int port, String username, String password, Boolean automaticRecovery, int retries, int timeout) {
        appLogger.info("Triggered: -");
        ConnectionFactory connectionFactory;
        try {
            connectionFactory = getConnectionFactory(server, port, username, password, automaticRecovery, timeout);
        } catch(Exception e) {
            appLogger.error("PublishOneMessage Exception: " + e.getMessage());
            return false;
        }
        return doPublishOneMessage(connectionFactory, message, exchangeName, exchangeType, queueName, routingKey, autoACK, retries);
    }//PublishOneMessage

    private void startPublisher() {
        int retries = mRetries;
        while(retries>0) {
            try {
                mConnection = pConnectionFactory.newConnection();
                mChannel = mConnection.createChannel();
                mChannel.exchangeDeclare(mExchangeName, mExchangeType, true);
                mChannel.confirmSelect();
                mChannel.queueDeclare(mQueueName, true, false, false, null);
                appLogger.info("Queue: " + mQueueName);
                mChannel.queueBind(mQueueName, mExchangeName, mRoutingKey);
                retries = 0;
            } catch (Exception e) {
                appLogger.error("startPublisher Error: " + e.getMessage());
                retries--;
                if(mChannel!=null) {
                    try {
                        mChannel.close();
                    } catch (IOException | TimeoutException ee) {
                        appLogger.error("Unable to close Channel: " + ee.getMessage());
                    }
                }
                if(mConnection!=null) {
                    try {
                        mConnection.close();
                    } catch (IOException ee) {
                        appLogger.error("Unable to close Connection: " + ee.getMessage());
                    }
                }
            }
        }
    }//startPublisher

    public boolean IsConnected() {
        return (mConnection!=null && mChannel!=null & mChannel.isOpen());
    }//IsConnected

    public RMQSimplePublisher(String connectionString, Boolean automaticRecovery, String exchangeName, String exchangeType, String queueName, String routingKey, int retries)
            throws Exception {
        this.pConnectionFactory = getConnectionFactory(connectionString, automaticRecovery);
        this.mExchangeName = exchangeName;
        this.mExchangeType = exchangeType;
        this.mQueueName = queueName;
        this.mRoutingKey = routingKey;
        this.mRetries = retries;
        startPublisher();
    }//RMQSimplePublisher

    public RMQSimplePublisher(String server, int port, String username, String password, Boolean automaticRecovery, String exchangeName, String exchangeType, String queueName, String routingKey, int retries, int timeout)
            throws Exception {
        this.pConnectionFactory = getConnectionFactory(server, port, username, password, automaticRecovery, timeout);
        this.mExchangeName = exchangeName;
        this.mExchangeType = exchangeType;
        this.mQueueName = queueName;
        this.mRoutingKey = routingKey;
        this.mRetries = retries;
        startPublisher();
    }//RMQSimplePublisher

    public boolean PublishMessage(String message, boolean autoACK) {
        appLogger.info("Triggered: -");
        if(!this.IsConnected()) {
            appLogger.warn("MultiPublisher not started!");
            startPublisher();
        }
        boolean result;
        try {
            mChannel.basicPublish(mExchangeName, mRoutingKey, null, message.getBytes());
            appLogger.info("Message published [s]");
            if(!autoACK) {
                mChannel.waitForConfirmsOrDie();//Confirms (aka Publisher Acknowledgements) throws IOException
            }
            result = true;
        } catch (IOException ioe) {
            appLogger.error("Message ACK error: " + ioe.getMessage());
            appLogger.debug("Message [f]: " + message);
            result = false;
        } catch (Exception e) {
            appLogger.error("Message ACK error: " + e.getMessage());
            appLogger.debug("Message [f]: " + message);
            result = false;
        }
        return result;
    }//MultiPublisherSendMessage

    public void Destroy() {
        appLogger.info("Triggered: -");
        if(mChannel!=null) {
            try {
                mChannel.close();
            } catch (IOException | TimeoutException e) {
                appLogger.error("Unable to close Channel: " + e.getMessage());
            }
        }
        if(mConnection!=null) {
            try {
                mConnection.close();
            } catch (IOException e) {
                appLogger.error("Unable to close Connection: " + e.getMessage());
            }
        }
    }//Destroy
}//RMQSimplePublisher
