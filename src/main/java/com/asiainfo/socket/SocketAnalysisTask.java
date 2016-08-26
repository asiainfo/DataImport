package com.asiainfo.socket;

import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangjing5 on 2016/4/22.
 */
public class SocketAnalysisTask implements Runnable {
    private final static Logger logger = Logger.getLogger(SocketAnalysisTask.class);
    public static LinkedBlockingQueue<Socket> socketQueue;
    public static LinkedBlockingQueue<byte[]> msgQueue;
    private Socket socket;
    private static int xdrRawDataCount = 0;

    public SocketAnalysisTask( Socket socket){
        this.socketQueue = SocketServer.socketQueue;
        this.msgQueue = FlumeSource.msgQueue;
        this.socket = socket;
    }

    private int msgType = 0;
    //    @Override
//    public void run() {
//        Socket socket = null;
//        byte[] contextArr = new byte[20];
//        int flag = 0;
//
//        while(true){
//            try {
//                socket = socketQueue.take();
//                BufferedReader is=new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                logger.info(is.readLine().getBytes());
//                msgQueue.put(is.readLine().getBytes());
//                flag++;
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }finally {
//                try {
//                    socket.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
    @Override
    public void run() {
        Socket socket = null;
        DataInputStream ds = null;
        OutputStream outputstream = null;
        logger.debug("ds...运营run方法");
     //   while (true) {
            try {
                //socket = socketQueue.take();
                socket = this.socket;
                outputstream = socket.getOutputStream();
                ds = new DataInputStream(socket.getInputStream());


                while (true) {

                    int preLen = ds.available( );

                    logger.info("get a msg " + preLen);
                    //try {


                    byte[] type = new byte[2];

                    byte[] len = new byte[2];
                    int msgType = 0;
                    int length = 0;


                    //消息的总长度，2个字节
                    ds.readFully(len);
                    length = SocketUtil.bytesToInt(len);
                    logger.info("length=" + length);

                    ds.readFully(type);
                    msgType = SocketUtil.bytesToInt(type);
                    logger.info("msgType=" + msgType);

                    //message type是版本协商请求，回复版本信息
                    byte[] sequenceId = new byte[4];

                    ds.readFully(sequenceId);
                    int ss = SocketUtil.bytes2Int(sequenceId);


                    byte ct = 0;

                    ct = ds.readByte();//包头均已读完

                    logger.info( "context" + ct );
                    if (msgType == 0x0001) {//版本请求
                        logger.info("msgType == 0x0001");
                        //byte[] clientVersion = new byte[2];
                        //ds.readFully(clientVersion);
                        ds.skipBytes(2);

                        outputstream.write(versionResponse(sequenceId));
                        outputstream.flush();
                    } else if (msgType == 0x0002) { //链路鉴权请求
                        logger.info("msgType == 0x0002");
                        ds.skipBytes(82);

                        outputstream.write(linkAuthResponse(sequenceId));
                        outputstream.flush();
                    } else if (msgType == 0x0003) {//链路检测请求
                        logger.info("msgType == 0x0003");
                        outputstream.write(linkCheckResponse(sequenceId));
                        outputstream.flush();
                    } else if (msgType == 0x0006) {//XDR对应原始数据传输请求
                        logger.info("msgType == 0x0006");
                        xdrRawDataCount++;
                        byte[] contextArr = new byte[length - 17];
                        ds.skipBytes(8);
                        ds.readFully(contextArr);
                        msgQueue.offer(contextArr);
                    //写入flume
//                    Event e = EventBuilder.withBody(contextArr);
//                    getChannelProcessor().processEvent(e);
//                    status = PollableSource.Status.READY;

                        outputstream.write(dataResponse(sequenceId));
                        outputstream.flush();
                    } else if (msgType == 0x0005) {//CDR对应原始数据传输请求
                        logger.info("msgType == 0x0005");
                        logger.info("i am here---------0x0005");
                        xdrRawDataCount++;
                        byte[] contextArr = new byte[length - 9];
                        //ds.skipBytes(10);
                        ds.readFully(contextArr);
                        msgQueue.offer(contextArr);

                    //写入flume
                    //Event e = EventBuilder.withBody(contextArr);
//                        Event e =  EventBuilder.withBody(contextArr).setHeaders();
//                    getChannelProcessor().processEvent(e);
//                    status = PollableSource.Status.READY;

                        outputstream.write(cdrdataResponse(sequenceId));
                        outputstream.flush();


                    } else if (msgType == 0x0007) {//链路数据发送校验请求
                        logger.info("msgType == 0x0007");
                        byte[] sendFlag = new byte[4];
                        ds.readFully(sendFlag);
                        byte[] sendCount = new byte[4];
                        ds.readFully(sendCount);

                        outputstream.write(linkDataCheckResponse(sequenceId, sendFlag, sendCount));
                        outputstream.flush();
                    } else if (msgType == 0x0004) {//连接释放请求
                        logger.debug("msgType == 0x0004");
                        //关闭连接
                        ds.skipBytes(1);
                        outputstream.write(relResponse(sequenceId));
                        outputstream.flush();
                        socket.close();
                        break;
                    }else{
                        socket.close();
                        logger.info(" wrong msgType");
                        throw new IOException();
                    }
                    //} catch (InterruptedException e) {
                    //e.printStackTrace();
                    //} catch (IOException e) {
                    //e.printStackTrace();
                    //}
                }
            }  catch (IOException e) {
                e.printStackTrace();
            }

        //}
    }

    public static void logReponse(byte[] byteToResponse){
        String hexString =  null;
        for ( int i = 0; i < byteToResponse.length; i++){
            hexString += Integer.toHexString(byteToResponse[i]);
        }
        logger.info(hexString);
    }


    public static byte[] versionResponse(byte[] sequenceId){
        byte[] msgVersion = new byte[10];
        logger.debug("i am here---------versionResponse");
        //length
        msgVersion[0] = (byte) 0x00;
        msgVersion[1] = (byte) 0x0a;
        //message type
        msgVersion[2] = (byte) 0x80;
        msgVersion[3] = (byte) 0x01;
        //sequenceId
        msgVersion[4] = sequenceId[0];
        msgVersion[5] = sequenceId[1];
        msgVersion[6] = sequenceId[2];
        msgVersion[7] = sequenceId[3];
        //TotalContents
        msgVersion[8] = (byte) 0x00;
        //判断客户端版本，回复1: 版本协商通过。2: 版本过高。3: 版本过低
        msgVersion[9] = (byte) 0x01;
        //logger.info(msgVersion);
        //logReponse(msgVersion);
        return msgVersion;
    }

    public static byte[] linkAuthResponse(byte[] sequenceId){
        byte[] linkAuthResp = new byte[74];
        //length
        logger.debug("i am here---------linkAuthResonse");
        linkAuthResp[0] = (byte) 0x00;
        linkAuthResp[1] = (byte) 0x4a;
        //message type
        linkAuthResp[2] = (byte) 0x80;
        linkAuthResp[3] = (byte) 0x02;
        //sequenceId
        linkAuthResp[4] = sequenceId[0];
        linkAuthResp[5] = sequenceId[1];
        linkAuthResp[6] = sequenceId[2];
        linkAuthResp[7] = sequenceId[3];
        //TotalContents
        linkAuthResp[8] = (byte) 0x00;
        //鉴权结果，1 代表鉴权通过。 2 代表LoginID不存在。3 代表SHA256加密结果出错
        linkAuthResp[9] = (byte) 0x01;
        for(int i=10;i<74;i++){
            linkAuthResp[i] = (byte) 0x00;
        }
        //logReponse(linkAuthResp);

        return linkAuthResp;
    }

    public static byte[] dataResponse(byte[] sequenceId){
        byte[] dataResp = new byte[10];
        //length
        logger.debug("i am here---------dataResponse");
        dataResp[0] = (byte) 0x00;
        dataResp[1] = (byte) 0x0a;
        //message type
        dataResp[2] = (byte) 0x80;
        dataResp[3] = (byte) 0x06;
        //sequenceId
        dataResp[4] = sequenceId[0];
        dataResp[5] = sequenceId[1];
        dataResp[6] = sequenceId[2];
        dataResp[7] = sequenceId[3];
        //TotalContents
        dataResp[8] = (byte) 0x00;
        //数据返回请求，1代表成功 其它 代表失败
        dataResp[9] = (byte) 0x01;
        return dataResp;
    }

    public static byte[] cdrdataResponse(byte[] sequenceId){
        byte[] dataResp = new byte[10];
        //length
        logger.debug("i am here---------cdrdataResponse");
        dataResp[0] = (byte) 0x00;
        dataResp[1] = (byte) 0x0a;
        //message type
        dataResp[2] = (byte) 0x80;
        dataResp[3] = (byte) 0x05;
        //sequenceId
        dataResp[4] = sequenceId[0];
        dataResp[5] = sequenceId[1];
        dataResp[6] = sequenceId[2];
        dataResp[7] = sequenceId[3];
        //TotalContents
        dataResp[8] = (byte) 0x00;
        //数据返回请求，1代表成功 其它 代表失败
        dataResp[9] = (byte) 0x01;
        return dataResp;
    }


    public static byte[] linkCheckResponse(byte[] sequenceId){
        byte[] linkCheckReps = new byte[9];
        //length
        logger.debug("i am here---------linCheckResponse");
        linkCheckReps[0] = (byte) 0x00;
        linkCheckReps[1] = (byte) 0x09;
        //message type
        linkCheckReps[2] = (byte) 0x80;
        linkCheckReps[3] = (byte) 0x03;
        //sequenceId
        linkCheckReps[4] = sequenceId[0];
        linkCheckReps[5] = sequenceId[1];
        linkCheckReps[6] = sequenceId[2];
        linkCheckReps[7] = sequenceId[3];
        //TotalContents
        linkCheckReps[8] = (byte) 0x00;
        return linkCheckReps;
    }
    public static byte[] linkDataCheckResponse(byte[] sequenceId,byte[] sendFlag,byte[] sendCount){
        byte[] linkDataCheckReps = new byte[22];
        //length
        logger.debug("i am here---------linkDataResponse");
        linkDataCheckReps[0] = (byte) 0x00;
        linkDataCheckReps[1] = (byte) 0x16;
        //message type
        linkDataCheckReps[2] = (byte) 0x80;
        linkDataCheckReps[3] = (byte) 0x07;
        //sequenceId
        linkDataCheckReps[4] = sequenceId[0];
        linkDataCheckReps[5] = sequenceId[1];
        linkDataCheckReps[6] = sequenceId[2];
        linkDataCheckReps[7] = sequenceId[3];
        //TotalContents
        linkDataCheckReps[8] = (byte) 0x00;
        //Sendflag
        linkDataCheckReps[9] = sendFlag[0];
        linkDataCheckReps[10] = sendFlag[1];
        linkDataCheckReps[11] = sendFlag[2];
        linkDataCheckReps[12] = sendFlag[3];
        int count = SocketUtil.bytes2Int(sendCount);
        if(count==xdrRawDataCount){
            linkDataCheckReps[13] = (byte) 0x00;
        }else if(count<xdrRawDataCount){
            linkDataCheckReps[13] = (byte) 0x01;
        }else if(count>xdrRawDataCount){
            linkDataCheckReps[13] = (byte) 0x02;
        }
        //SendDataInfo,发送的数据包为0
        //byte[] send = SocketUtil.int2Byte(0);
        //linkDataCheckReps[14] = send[0];
        //linkDataCheckReps[15] = send[1];
        //linkDataCheckReps[16] = send[2];
        //linkDataCheckReps[17] = send[3];
        linkDataCheckReps[14] = sendCount[0];
        linkDataCheckReps[15] = sendCount[1];
        linkDataCheckReps[16] = sendCount[2];
        linkDataCheckReps[17] = sendCount[3];
        byte[] rec = SocketUtil.int2Byte(xdrRawDataCount);
        //RecDataInfo
        linkDataCheckReps[18] = rec[0];
        linkDataCheckReps[19] = rec[1];
        linkDataCheckReps[20] = rec[2];
        linkDataCheckReps[21] = rec[3];
        return linkDataCheckReps;
    }


    public static byte[] relResponse(byte[] sequenceId){
        byte[] msgVersion = new byte[10];
        //length
        logger.debug("i am here---------relResponse");
        msgVersion[0] = (byte) 0x00;
        msgVersion[1] = (byte) 0x0a;
        //message type
        msgVersion[2] = (byte) 0x80;
        msgVersion[3] = (byte) 0x04;
        //sequenceId
        msgVersion[4] = sequenceId[0];
        msgVersion[5] = sequenceId[1];
        msgVersion[6] = sequenceId[2];
        msgVersion[7] = sequenceId[3];
        //TotalContents
        msgVersion[8] = (byte) 0x00;
        //连接释放的完成状态 1：释放完成  其它：释放失败。
        msgVersion[9] = (byte) 0x01;
        return msgVersion;
    }
}




