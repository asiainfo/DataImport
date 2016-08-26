package com.asiainfo.tail;

import com.asiainfo.util.common.DateUtil;
import com.asiainfo.util.common.StringUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by yangjing5 on 2016/4/26.
 */
public class TailFileSource extends AbstractSource implements EventDrivenSource,Configurable {
    private static final Logger logger = LoggerFactory.getLogger(TailFileSource.class);
    private SourceCounter sourceCounter;
    private String pointerFile;
    private String tailFile;
    private String tailPath;
    private String currentFile;
    private long collectInterval;
    private int batchLine;
    private boolean batch;
    private AtomicBoolean tailRun=new AtomicBoolean(true);
    private AtomicLong cursor=new AtomicLong(0);

    @Override
    public void configure(Context context) {
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
        pointerFile = context.getString("tailfile.pointer","/home/ocdp/yj/cursor.pt");
        currentFile = context.getString("tailfile.current","/home/ocdp/yj/current");
        tailPath = context.getString("tailfile.path","/home/ocdp/yj/testdata/test");
        collectInterval=context.getLong("tailfile.collectInterval",10L);
        batchLine=context.getInteger("tailfile.batchLine",50);
        batch=context.getBoolean("tailfile.batch",true);
    }
    @Override
    public synchronized void start() {
        super.start();
        sourceCounter.start();
        TailFile tf=new TailFile();
        tf.addFileTailerListener(new FileTailerListener(){
            @Override
            public void newFileLine(List<Event> events) {
                if(!events.isEmpty()){
                    getChannelProcessor().processEventBatch(events);
                    sourceCounter.incrementAppendBatchAcceptedCount();
                    sourceCounter.addToEventAcceptedCount(events.size());
                }
            }
            @Override
            public void newFileLine(Event event) {
                getChannelProcessor().processEvent(event);
                sourceCounter.incrementAppendAcceptedCount();
            }
        });
        Thread t=new Thread(tf);
        t.setDaemon(true);
        t.start();
    }
    @Override
    public synchronized void stop() {
        tailRun.set(false);
        super.stop();
        sourceCounter.stop();
    }

    protected interface FileTailerListener{
        public void newFileLine(List<Event> events);
        public void newFileLine(Event event);
    }

    protected class PointNode{
        String fileName;
        long cursor;
        public PointNode(String fileName,long cursor){
            this.fileName = fileName;
            this.cursor = cursor;
        }
    }


    protected class TailFile implements Runnable{
        private Set<FileTailerListener> listeners = new HashSet<FileTailerListener>();


        public void addFileTailerListener(FileTailerListener l) {
            this.listeners.add(l);
        }
        public void removeFileTailerListener(FileTailerListener l) {
            this.listeners.remove(l);
        }
        @Override
        public void run() {
            //long test = new File("/mnt/video/58/u_ex16042814.log").length();
//            long[] temp={0L,0L};
//            this.writePointerFile(temp);
//            SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHH");
//            String nowHour = sdf.format(new Date());
//            tailFile = tailPath+"/u_ex"+nowHour+".log";
            tailFile = this.readCurrentFile();
            System.out.println("[***read file****]:"+tailFile);
            long[] st=this.readPointerFile();
            RandomAccessFile file=null;
            boolean flag=true;
            File tf = null;
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while(flag){
                try {
                    tf=new File(tailFile);
                    file = new RandomAccessFile(tf, "r");
                    cursor.set(st[1]);
//                    if(st[0]==tf.lastModified()){
//                        cursor.set(st[1]);
//                    }else{
//                        st[0]=tf.lastModified();
//                        cursor.set(0);
//                    }
                    flag=false;
                } catch (Exception e) {
                    try {
                        logger.error(e.getMessage()+",will retry file:"+tailFile);
                        Thread.sleep(5000);
                    } catch (Exception e1) {

                    }
                }
            }

            while(tailRun.get()){
                try {
//                    if(!this.sameTailFile(st[0])) {
//                        logger.error("file change:"+tailFile);
//                        tf=new File(tailFile);
//                        file = new RandomAccessFile(tf, "r");
//                        st[0]=tf.lastModified();
//                    }
                    long fileLength =file.length();
                    //日志文件没有写操作或者日志文件发生切割
                    if (fileLength == cursor.longValue()) {

                        File newFile = this.getNewFile(tf.getParent());
                        if(newFile.exists()) {
                            if (!newFile.getName().equals(tf.getName()) && (fileLength == cursor.longValue())) {
                                System.out.println("[***split file name***]:" + newFile.getName());
                                //日志文件切割，游标从0计数
                                cursor.set(0);
                                tailFile = newFile.getAbsolutePath();
                                tf = new File(tailFile);
                                file = new RandomAccessFile(tf, "r");
                                st[0] = tf.lastModified();
                                writePointerFile(st);
                                this.writeCurrentFile(tailFile);
                            }
                        }
                    }
                    //日志文件没有切割
                    if (fileLength > cursor.longValue()) {
                        file.seek(cursor.get());
                        String line = file.readLine();
                        int i=1;
                        if(batch){
                            List<Event> batchAl=new ArrayList<Event>(batchLine);
                            while (line != null) {
                                //batchAl.add(EventBuilder.withBody(line.getBytes()));
                                //业务处理
                                String dealLine = dealLine(line);
                                if(!"".equals(dealLine)){
                                    batchAl.add(EventBuilder.withBody(dealLine.getBytes()));
                                }
                                line = file.readLine();
                                if(i%batchLine==0||line==null) {
                                    fireNewFileLine(batchAl);
                                    batchAl.clear();
                                    cursor.set(file.getFilePointer());
                                    st[1]=cursor.get();
                                    writePointerFile(st);
                                    this.writeCurrentFile(tailFile);
                                }
                                i++;
                            }
                            batchAl.add(EventBuilder.withBody(line.getBytes()));
                            if(!batchAl.isEmpty()){
                                fireNewFileLine(batchAl);
                                batchAl.clear();
                                cursor.set(file.getFilePointer());
                                st[1]=cursor.get();
                                writePointerFile(st);
                            }
                        }else{
                            while(line!=null){
                                fireNewFileLine(EventBuilder.withBody(line.getBytes()));
                                line = file.readLine();
                            }
                            cursor.set(file.getFilePointer());
                            st[1]=cursor.get();
                            writePointerFile(st);
                        }
                    }
                    Thread.sleep(collectInterval);
                } catch (Exception e) {

                }

            }

            try {
                if(file!=null) file.close();
            } catch (Exception e) {

            }
        }

        private File getNewFile(String path){
            SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHH");
            String nowHour = sdf.format(new Date());
            String nextHour = sdf.format(new Date().getTime()+60 * 60 * 1000);
            File nextHourFile = new File(path+"/u_ex"+nextHour+".log");
            File nowHourFile = new File(path+"/u_ex"+nowHour+".log");
            if(nextHourFile.exists()){
                return nextHourFile;
            }
//            long dateNew = currentFile.lastModified();
//            if(file.isDirectory()){
//                File[] files = file.listFiles();
//                for (int i = 0; i < files.length; i++) {
//                    if(files[i].lastModified()>=dateNew){
//                        return new File(files[i].getAbsolutePath());
//                    }
//                }
//            }
            return nowHourFile;
        }

        private long[] readPointerFile(){
            logger.info("read pointerFile:"+pointerFile);
            ObjectInputStream ois=null;
            long[] temp={0L,0L};
            try {
                ois=new ObjectInputStream(new FileInputStream(new File(pointerFile)));
                temp[0]=ois.readLong();
                temp[1]=ois.readLong();
            } catch (Exception e) {
                logger.error("can't read pointerFile:"+pointerFile);
            } finally{
                try {
                    if(ois!=null)ois.close();
                } catch (Exception e) {

                }
            }
            return temp;
        }

        private void writePointerFile(long[] temp){
            logger.debug("write pointerFile:"+pointerFile);
            ObjectOutputStream oos=null;
            try {
                oos=new ObjectOutputStream(new FileOutputStream(pointerFile));
                oos.writeLong(temp[0]);
                oos.writeLong(temp[1]);
            } catch (Exception e) {
                logger.error("can't write pointerFile:"+pointerFile);
            }finally{
                try {
                    if(oos!=null)oos.close();
                } catch (Exception e) {

                }
            }

        }

        private String readCurrentFile(){
            File file = new File(currentFile);
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = reader.readLine();
                if(tempString != null){
                    return tempString;
                }
            }catch (Exception e) {
                logger.error("can't read curentFile:" + currentFile);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHH");
            String nowHour = sdf.format(new Date());
            String tempString = tailPath+"/u_ex"+nowHour+".log";
            return tempString;
        }

        private void writeCurrentFile(String fileName){
            FileWriter fileWritter = null;
            BufferedWriter bufferWritter = null;
            try {
                fileWritter = new FileWriter(currentFile);
                bufferWritter = new BufferedWriter(fileWritter);
                bufferWritter.write(fileName);
            } catch (Exception e) {
                logger.error("can't write curentFile:" + currentFile);
            }finally {
                try {
                    bufferWritter.close();
                    fileWritter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private boolean sameTailFile(long time){
            return new File(tailFile).lastModified()==time?true:false;
        }


        protected void fireNewFileLine(List<Event> events) {
            for (Iterator<FileTailerListener> i = this.listeners.iterator(); i.hasNext();) {
                FileTailerListener l =  i.next();
                l.newFileLine(events);
            }
        }


        protected void fireNewFileLine(Event event) {
            for (Iterator<FileTailerListener> i = this.listeners.iterator(); i.hasNext();) {
                FileTailerListener l =  i.next();
                l.newFileLine(event);
            }
        }

        private String dealLine(String line) {
            // 处理每行数据
            if (line == null) {
                return "";
            } else if (StringUtil.clear(line).startsWith("#")) {
                return "";
            } else {
                // 读取的一行有内容，进行处理
                // Step 1：以tab进行拆分，分离出时间和内容
                String[] timeAndContent = StringUtil.clear(line).split(" ");
                if (timeAndContent.length >= 6) {
                    Date time = new Date();
                    try {
                        time = DateUtil.parseDateTime(StringUtil.clear(timeAndContent[0] + " " + timeAndContent[1]));
                        time = DateUtil.parseDateTime(DateUtil.addHours(time, 8));
                    } catch (Exception e) {
                    }
                    String ip = StringUtil.clear(timeAndContent[2]);
                    // Step 2：以&进行分割，拆分内容为键值对
                    String[] keyValues = StringUtil.clear(timeAndContent[5]).split("&");
                    if (keyValues != null) {
                        Map<String, String> valueMap = new HashMap<String, String>();
                        for (int i = 0; i < keyValues.length; i++) {
                            // Step 3：以=进行分割，获取具体值
                            String[] keyAndValue = StringUtil.clear(keyValues[i]).split("=");
                            if (keyAndValue.length == 2) {
                                String key = keyAndValue[0];
                                String value = keyAndValue[1];

                                // 仅处理uid、v、vd、u、gscm和n的值
                                if (StringUtil.compare(key, "uid", true) || StringUtil.compare(key, "v", true) ||
                                        StringUtil.compare(key, "vd", true) || StringUtil.compare(key, "u", true) ||
                                        StringUtil.compare(key, "gscm", true) || StringUtil.compare(key, "n", true)) {
                                    valueMap.put(key, value);
                                }
                            }
                        }
                        String uid = StringUtil.clear(valueMap.get("uid"));
                        if (StringUtil.isEmpty(uid) || StringUtil.compare(uid, "-", true)) {
                            if (!StringUtil.isEmpty(valueMap.get("v"))) {
                                uid = StringUtil.clear(valueMap.get("v"));
                            }
                        }
                        String title = StringUtil.clear(valueMap.get("n"));
                        try {
                            title = URLDecoder.decode(StringUtil.clear(valueMap.get("n")), "UTF-8");
                        } catch (Exception e) {

                        }
                        return DateUtil.parseDateTime(time) + "|" + ip + "|" + uid + "|" + StringUtil.clear(valueMap.get("vd")) + "|" + title;
                    }
                }
                return "";
            }
        }
    }
}
