package com.asiainfo.tail;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ai on 2016/4/27.
 */
public class MysqlSink extends AbstractSink implements Configurable {
    private Logger LOG = LoggerFactory.getLogger(MysqlSink.class);
    private String hostname;
    private String port;
    private String databaseName;
    private String tableName;
    private String user;
    private String password;
    private PreparedStatement preparedStatement;
    private Connection conn;
    private int batchSize;
    public MysqlSink() {
        LOG.info("MysqlSink start...");
    }
    @Override
    public void configure(Context context) {
        hostname = context.getString("hostname");
        port = context.getString("port");
        databaseName = context.getString("databaseName");
        tableName = context.getString("tableName");
        user = context.getString("user");
        password = context.getString("password");
        batchSize = context.getInteger("batchSize", 100);
    }
    @Override
    public void start() {
        super.start();
        try {
            //调用Class.forName()方法加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName;
        //调用DriverManager对象的getConnection()方法，获得一个Connection对象
        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            //创建一个Statement对象
            preparedStatement = conn.prepareStatement("insert into " + tableName + " (ID,NAME) values (?,?)");

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    @Override
    public void stop() {
        super.stop();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        List<String> actions = new ArrayList();
        transaction.begin();
        try {
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    content = new String(event.getBody());
                    actions.add(content);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            if (actions.size() > 0) {
                preparedStatement.clearBatch();
                for (String temp : actions) {
                    String[] tmp = temp.split(",");
                    String id = tmp[0];
                    String name = tmp[1];
                    preparedStatement.setString(1, id);
                    preparedStatement.setString(2, name);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
            transaction.commit();
        } catch (Throwable e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" + "successful.", e2);
            }
            LOG.error("Failed to commit transaction." + "Transaction rolled back.", e);
        } finally {
            transaction.close();
        }
        return result;
    }
}

