package com.crm.test.SinkToJDBCBatch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 普通JDBC Sink
 */
public class JDBCSink extends RichSinkFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //加载驱动
        Class.forName("com.mysql.jdbc.Driver");
        //连接初始化
//        String url = "jdbc:mysql://134.96.226.230:9030,134.96.226.229:9030,134.96.226.228:9030/ZH_MSS";
        String url = "jdbc:mysql://10.10.226.241:16033/ZH_MSS?rewriteBatchedStatements=true";
        connection = DriverManager.getConnection(url, "dwetl_mss", "1cjWL_NxqYe");
        preparedStatement = connection.prepareStatement("select 1");
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(String SQL, Context context) throws Exception {
        try {
            preparedStatement.execute(SQL);
        } catch (Exception e) {
            System.out.println("异常SQL:" + SQL);
            throw e;
        }
    }

}
