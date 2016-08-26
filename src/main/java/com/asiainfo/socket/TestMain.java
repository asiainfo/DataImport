package com.asiainfo.socket;

import java.io.File;

/**
 * Created by ai on 2016/4/26.
 */
public class TestMain {
    public static void main(String[] args){
       readPointerFile();
        //System.out.println(newFile);
    }
    private static long[] readPointerFile(){
        java.io.ObjectInputStream ois=null;
        long[] temp={0L,0L};
        try {
            ois=new java.io.ObjectInputStream(new java.io.FileInputStream(new File("/home/ocdp/app/cursor.pt")));
            temp[0]=ois.readLong();
            temp[1]=ois.readLong();
            System.out.println(temp[0]+"======="+temp[1]);
            System.out.println(new File("/mnt/video/58/u_ex16042817.log").length());
        } catch (Exception e) {
        } finally{
            try {
                if(ois!=null)ois.close();
            } catch (Exception e) {

            }
        }
        return temp;
    }
}
