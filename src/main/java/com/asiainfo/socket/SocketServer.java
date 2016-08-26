package com.asiainfo.socket;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangjing5 on 2016/4/18.
 */
public class SocketServer implements  Runnable {
    public static LinkedBlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>(Integer.MAX_VALUE);
    private final static Logger logger = Logger.getLogger(SocketServer.class);
    private String socketPort;
    public SocketServer(String socketPort) {
        this.socketPort = socketPort;
    }
    @Override
    public void run() {
        ServerSocket server=null;
        try {
            server=new ServerSocket(Integer.parseInt(socketPort));
        } catch (IOException e) {
            logger.error("could not listen ",e);
        }
        Socket socket=null;
        while(true) {
            try {

                socket = server.accept();
                socket.setReceiveBufferSize(10 * 10240 * 1024);
                //socketQueue.put(socket);
                new Thread(new SocketAnalysisTask(socket)).start();

            } catch (IOException e) {
                logger.error("could not accept data from client",e);
            }
        }
    }
}
