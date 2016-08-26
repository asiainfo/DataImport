package com.asiainfo.tail;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ai on 2016/4/26.
 */
public class TailFileClient {
    public static void main(String[] args){
        String filePath = "/home/ocdp/yj/testdata/test";
        String splitPath = "/home/ocdp/yj/test.tmp";
        int spilt = 2000;
        int count = 0;
        try {
            FileWriter fw = new FileWriter(filePath);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while(true){
                count++;
                fw.write(count+",hello" + "\n");
                if(count%100==0){
                    fw.flush();
                    Thread.sleep(1000);
                }
                if(count%spilt==0){
                    System.out.println("split=="+df.format(new Date()));
                    filePath = "/home/ocdp/yj/testdata/test"+count;
                    fw = new FileWriter(filePath);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
