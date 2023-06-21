package com.crm;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.crm.function.jsonParserProcess;
import com.lake.bean.TableMapperBean;
import com.lake.process.SideOutPutBuilder;
import com.lake.sink.IcebergSinkBuilder;
import com.lake.utils.FlinkEnvUtil;
import com.lake.utils.IcebergUtil;
import com.lake.utils.KafkaUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaToIcebergMain {
    public static void main(String[] args) throws Exception {
        InputStream propertiesInputStream = KafkaToIcebergMain.class.getResourceAsStream("/config.properties");

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesInputStream)
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));

        StreamExecutionEnvironment env = FlinkEnvUtil.creatEnv5();

        //配置文件放入流式环境
        env.getConfig().setGlobalJobParameters(parameterTool);

        SingleOutputStreamOperator<String> kafkaSourceStream = new KafkaUtil<String>(parameterTool)
                .source()
                .buildWithDeserializer(new SimpleStringSchema())
                .getDataStream(env);


        //加载catalog
        Configuration configuration = new Configuration();
        configuration.addResource(KafkaToIcebergMain.class.getResourceAsStream("/core-site.xml"));
        configuration.addResource(KafkaToIcebergMain.class.getResourceAsStream("/hdfs-site.xml"));
        configuration.addResource(KafkaToIcebergMain.class.getResourceAsStream("/hive-site.xml"));
        CatalogLoader catalogLoader = IcebergUtil.getCatalogLoader(parameterTool, configuration);
        List<TableMapperBean> tableMapperBeanList = analyzeTableMapper();

        SingleOutputStreamOperator parseStream = new SideOutPutBuilder(tableMapperBeanList, kafkaSourceStream, catalogLoader, parameterTool)
                .parse(new jsonParserProcess(tableMapperBeanList, parameterTool.get("iceberg.namespace.name"), catalogLoader));

        IcebergSinkBuilder
                .of(tableMapperBeanList, parseStream, parameterTool)
                .build(catalogLoader);

        env.execute("Kafka To Iceberg");


    }

    private static Map<String, String> getenv() {
        Map<String, String> map = new HashMap();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    private static List<TableMapperBean> analyzeTableMapper() throws Exception {
        InputStream tableMapperInputStream = KafkaToIcebergMain.class.getResourceAsStream("/tableMapper");
        BufferedReader reader = new BufferedReader(new InputStreamReader(tableMapperInputStream));
        ArrayList<TableMapperBean> tableMapperBeans = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            // 对读入的每一行数据执行相应的操作
            String[] mapperSplit = line.split(" ");
            if (mapperSplit.length >= 3) {
                tableMapperBeans.add(new TableMapperBean(mapperSplit[0], mapperSplit[1], mapperSplit[2]));
            } else {
                tableMapperBeans.add(new TableMapperBean(mapperSplit[0], mapperSplit[1], null));
            }
        }
        if (tableMapperBeans.size() == 0) {
            throw new Exception("tableMapper File is invalid, or size is 0");
        } else {
            return tableMapperBeans;
        }
    }
}
