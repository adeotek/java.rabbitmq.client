package com.hinter.java.rabbitmq.client;

public interface IMessageHandler {
    public boolean ProcessMessage(byte[] body, String queueName, String routingKey);
}//IMessageHandler
