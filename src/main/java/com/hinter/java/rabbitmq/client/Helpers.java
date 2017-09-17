package com.hinter.java.rabbitmq.client;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;

public final class Helpers {
    public static boolean isStringEmptyOrNull(String input, boolean trimInput) {
        if(input == null) {
            return true;
        }
        if(trimInput) {
            return input.trim().length()==0;
        }
        return input.length()==0;
    }//isStringEmptyOrNull

    public static boolean isStringEmptyOrNull(String input) {
        return isStringEmptyOrNull(input, true);
    }//isStringEmptyOrNull

    public static boolean isPortOpen(String hostname, int port, int timeout) {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(hostname, port), timeout);
            socket.close();
            return true;
        } catch(ConnectException ce) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }//isPortOpen
}//Helpers
