package com.asiainfo.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by yangjing5 on 2016/4/18.
 */
public class SocketClient {
    public static void main(String[] args){
        Socket s = null;
        try {

            int i =0;

            while(true) {
                i++;
                {
                    s = new Socket("10.131.129.135", 5001);
                    OutputStream os = s.getOutputStream();
                    String data = "hello"+i;
                    System.out.println("向服务端发送数据：" + data);
                    os.write(data.getBytes());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    s.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
