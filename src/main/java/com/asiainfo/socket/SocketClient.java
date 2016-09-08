package com.asiainfo.socket;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

/**
 * Created by peng on 16/8/17.
 */
public class SocketClient {

    public static void main(String[] args) throws Exception{

        Socket clientSocket = new Socket(args[0], Integer.parseInt(args[1]));
        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
        BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));


        long startTime=System.currentTimeMillis();
        for (int i = 0; i < Integer.valueOf(args[2]); i++) {
            outToServer.write(getMessage());
            System.out.print("send success\n");
            in.readLine();
        }


        clientSocket.close();

        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("End time : " + new Date());
        System.out.println("Take <" + (endTime-startTime)+"> ms");


        //getMessage();


    }

    public static byte[] getMessage(){
        Random r = new Random();
        String msg="";
        long beginTime = System.currentTimeMillis();
        int randint = r.nextInt(10);
        long imsi = 4600092691019400L + randint;
        String imsi_04 = String.valueOf(imsi);
        String beginTime_01 = String.valueOf(beginTime);
        String endTime_02 = String.valueOf(beginTime + 1000 * 60);
        long phonenum = 13891800000L + randint;
        long phonenumTo = 13891800000L + randint + 10;
        String phonenum_03 = String.valueOf(phonenum);
        String phonenumTo_13 = String.valueOf(phonenumTo);
        int datatype = r.nextInt(5) + 1;
        String datatype_05 = String.valueOf(datatype);
        long lac = 31250 + r.nextInt(3);
        String lac_06 = String.valueOf(lac);
        long cell = 5125;
        String cell_07 = String.valueOf(cell);
        long endlac = 31250 + r.nextInt(3);
        String endlac_08 = String.valueOf(endlac);
        long endcell = 5125;
        String endcell_09 = String.valueOf(endcell);
        msg = beginTime_01 + "|" + endTime_02 + "|" + phonenum_03 + "|" + imsi_04 + "|" + datatype_05 + "|" + lac_06 + "|" + cell_07 + "|" + endlac_08 + "|" + endcell_09 + "|" + "10" + "|" + "11" + "|" + "12" + "|" + phonenumTo_13 + "|" + "14" + "|" + "15" + "|" + "16" + "|" + "17";
        //byte[] head = hexStringToBytes("08ba00050000000228");
        //msg = new String(head) + msg;
        byte[] bytesmsg = msg.getBytes();
        System.out.println(msg);
        //System.out.print(msg.split("\\|")[3]);
        return bytesmsg;
    }
    private static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            //将一个byte的二进制数转换成十六进制字符
            String hv = Integer.toHexString(v);
            //如果二进制数转换成十六进制数高位为0，则加入'0'字符
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }

    public static byte[] hexStringToBytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase(Locale.getDefault());
        int length = hexString.length() / 2;
        //将十六进制字符串转换成字符数组
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
        //一次去两个字符
            int pos = i * 2;
        //两个字符一个对应byte的高四位一个对应第四位
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }
    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }
}
