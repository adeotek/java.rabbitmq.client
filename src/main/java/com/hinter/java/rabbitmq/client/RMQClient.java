package com.hinter.java.rabbitmq.client;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RMQClient {
    private static final Logger appLogger = LogManager.getLogger(RMQClient.class);

    protected ConnectionFactory pConnectionFactory;

    protected boolean isSubscriberStarted = false;
    protected Connection subscriberConnection = null;
    protected Channel subscriberChannel = null;

    public RMQClient(String connectionString, Boolean automaticRecovery)
            throws Exception {
        if (Helpers.isStringEmptyOrNull(connectionString)) {
            throw new Exception("Invalid connection string!");
        }
        this.pConnectionFactory = new ConnectionFactory();
        if(automaticRecovery!=null) {
            this.pConnectionFactory.setAutomaticRecoveryEnabled(automaticRecovery);
        }
        this.pConnectionFactory.setUri(connectionString);
    }//RMQClient

    public RMQClient(String server, int port, String username, String password, Boolean automaticRecovery, int timeout)
            throws Exception {
        if (Helpers.isStringEmptyOrNull(server)) {
            throw new Exception("Invalid connection parameters: server");
        }
        if (port<=0) {
            throw new Exception("Invalid connection parameters: port");
        }
        if (!Helpers.isPortOpen(server, port, timeout)) {
            throw new Exception("RabbitMQ server [" + server + ":" + port + "] is unreachable!");
        }
        this.pConnectionFactory = new ConnectionFactory();
        if(automaticRecovery!=null) {
            this.pConnectionFactory.setAutomaticRecoveryEnabled(automaticRecovery);
        }
        if (!Helpers.isStringEmptyOrNull(username)) {
            this.pConnectionFactory.setUsername(username);
            this.pConnectionFactory.setPassword((Helpers.isStringEmptyOrNull(password) ? "" : password));
        }
        this.pConnectionFactory.setHost(server);
        this.pConnectionFactory.setPort(port);
    }//RMQClient

    public boolean StartSubscriber(final IMessageHandler handler, final Boolean autoACK, final String exchangeName, final String exchangeType, final String queueName, final String routingKey, boolean resetIfStarted) throws Exception {
        appLogger.info("Triggered: -");
        if (handler==null) {
            throw new Exception("Invalid message handler");
        }
        if((isSubscriberStarted && subscriberConnection!=null && subscriberConnection.isOpen()) && !resetIfStarted) {
            appLogger.info("Subscriber is already started");
            return true;
        }
        if(isSubscriberStarted && resetIfStarted) {
            StopSubscriber();
        }
        int retries = 0;
        while (!isSubscriberStarted && retries<3) {
            appLogger.info("Subscriber connection while is triggered");
            try {
                subscriberConnection = pConnectionFactory.newConnection();
                subscriberChannel = subscriberConnection.createChannel();
                subscriberChannel.exchangeDeclare(exchangeName, exchangeType, true);
                subscriberChannel.confirmSelect(); // Enables publisher acknowledgements on this channel
                subscriberChannel.basicQos(1); // Maximum number of messages that the server will deliver, 0 if unlimited
                subscriberChannel.queueDeclare(queueName, true, false, false, null);
                subscriberChannel.queueBind(queueName, exchangeName, routingKey);
                final Consumer consumer = new DefaultConsumer(subscriberChannel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        appLogger.info("New message received");
                        boolean success = false;
                        try {
                            success = handler.ProcessMessage(body, queueName, routingKey);
                            if (!success) {
                                appLogger.error("Consumer error: failed to process message");
                                appLogger.debug("Message: " + (new String(body, "UTF-8")));
                            }
                        } catch (Exception e) {
                            appLogger.error("Consumer Exception: " + e.getMessage());
                            appLogger.debug("Message: " + (new String(body, "UTF-8")));
                        } finally {
                            appLogger.info("Message processed: sending ACK");
                            if (autoACK || success) {
                                subscriberChannel.basicAck(envelope.getDeliveryTag(), false);
                            }
                        }
                    }
                };
//                subscriberChannel.basicConsume(queueName, autoACK, consumer);
                subscriberChannel.basicConsume(queueName, autoACK, routingKey, consumer);//see autoACK param
                appLogger.warn("Subscriber started: Waiting for messages...");
                isSubscriberStarted = true;
            } catch (TimeoutException toe) {
                appLogger.error("Connection TimeoutException: " + toe.getMessage());
                isSubscriberStarted = false;
                retries++;
            } catch (Exception e) {
                appLogger.error("Connection Exception: " + e.getMessage());
                isSubscriberStarted = false;
                retries++;
            } finally {
                if (!isSubscriberStarted) {
                    try {
                        Thread.sleep(5000); //sleep and then try again
                    } catch (InterruptedException ie) {
                        appLogger.error("InterruptedException: " + ie.getMessage());
                        break;
                    }
                }
            }
        }
        return isSubscriberStarted;
    }//StartSubscriber

    public void StopSubscriber() {
        appLogger.info("Triggered: -");
        if (isSubscriberStarted) {
            isSubscriberStarted = false;
            if(subscriberChannel!=null) {
                try {
                    subscriberChannel.close();
                } catch (IOException | TimeoutException e) {
                    appLogger.error("Unable to close Channel: " + e.getMessage());
                } finally {
                    subscriberChannel = null;
                }
            }
            if(subscriberConnection!=null) {
                try {
                    subscriberConnection.close();
                } catch (IOException e) {
                    appLogger.error("Unable to close Channel: " + e.getMessage());
                } finally {
                    subscriberConnection = null;
                }
            }
        }
    }//StopSubscriber

    public boolean IsSubscriberOnline() {
        return (subscriberConnection!=null && subscriberConnection.isOpen() && isSubscriberStarted);
    }//IsSubscriberOnline
}//RMQClient
