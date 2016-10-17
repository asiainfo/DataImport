package com.asiainfo.socket;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangjing5 on 2016/4/18.
 */
public class FlumeSource extends AbstractSource implements Configurable,PollableSource {
    private final static Logger logger = Logger.getLogger(FlumeSource.class);
    //public static LinkedBlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>(Integer.MAX_VALUE);
    public static LinkedBlockingQueue<byte[]> msgQueue = new LinkedBlockingQueue<byte[]>(Integer.MAX_VALUE);
    //private ExecutorService threadPool = Executors.newCachedThreadPool();
    private int analysisNum = 100;
    private String socketPort;
    private String socketIP;
    private static int flag = 0;
    private static final String HEADERS_KEY = "key";
    private static final String MESSAGE_SEPARATOR = "\n";
    private static final String HEADERS_KEY_SEPARATOR = "\\|";


    @Override
    public void configure(Context context) {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(Constants.PROPERTIE_FILENAME);
        Properties props = new Properties();
        String port ;

        int recvworks;
        try {
            props.load(inputStream);
            analysisNum = Integer.parseInt(props.getProperty(Constants.ANALYSIS_THREAD_NUM));
            socketPort = props.getProperty(Constants.SOCKETSERVER_PORT);
            port = context.getString("port","5001");
            socketPort = port;
            logger.info("socketPort ="+socketPort);
            logger.info("load config ["+Constants.PROPERTIE_FILENAME+"]");

            recvworks = context.getInteger("maxworks", analysisNum);
            analysisNum = recvworks;

        } catch (IOException e) {
            logger.error("could not load config",e);
        }
    }

    @Override
    public void start() {
        String [] ports = socketPort.split(" ");

        for (String port : ports) {
            logger.info("port is " + port);
            new Thread(new SocketServer(port)).start();
        }
        // new Thread(new SocketServer(socketPort)).start();
        //for (int i =0 ;i< analysisNum;i++){
        //    new Thread(new SocketAnalysisTask()).start();
        //}
        logger.info("server socket,analysis task start...");
    }

    @Override
    public void stop () {
        // Disconnect from external client and do any additional cleanup
        // (e.g. releasing resources or nulling-out field values) ..
        super.stop();
    }

        @Override
    public Status process() {
            try {
                flag++;
                byte[] msg = msgQueue.take();
                long startTime=System.currentTimeMillis();
                String message = new String(msg);
                String[] messages = message.split(MESSAGE_SEPARATOR);

                for (String signalling : messages){
                    Map<String, String> headers = new HashMap();
                    //第4字段为imsi,作为key
                    String imsi = signalling.trim().split(HEADERS_KEY_SEPARATOR)[3];
                    try{
                        headers.put(HEADERS_KEY, imsi);
                    }catch (StringIndexOutOfBoundsException e){
                        logger.error("The message format is invalid. <" + signalling + ">");
                        continue;
                    }
                    //去除imsi为0的无效信令
                    if (!imsi.equals("000000000000000")){
                        Event e = EventBuilder.withBody(signalling.getBytes(), headers);
                        getChannelProcessor().processEvent(e);
                    }
                }

                //Event e = EventBuilder.withBody(msg);
                //getChannelProcessor().processEvent(e);

              long endTime = System.currentTimeMillis(); //获取结束时间
                logger.info("End time : " + new Date());

                logger.info("Take time: " + (endTime-startTime)+"ms");
                return Status.READY;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return Status.BACKOFF;
            }
        }
}