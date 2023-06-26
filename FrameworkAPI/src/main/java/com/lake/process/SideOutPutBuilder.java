package com.lake.process;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import com.lake.bean.TableMapperBean;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.List;

public class SideOutPutBuilder {
    private ParameterTool parameterTool;
    private List<TableMapperBean> tableMapperBeanList;
    private DataStream inputStream;
    private CatalogLoader catalogLoader;

    public SideOutPutBuilder(List<TableMapperBean> tableMapperBeanList, DataStream inputStream, CatalogLoader catalogLoader, ParameterTool parameterTool) {
        this.tableMapperBeanList = tableMapperBeanList;
        this.inputStream = inputStream;
        this.catalogLoader = catalogLoader;
        this.parameterTool = parameterTool;
    }

    public SingleOutputStreamOperator parse(BaseSideOutPutProcess baseSideOutPutProcess) {
        return inputStream.process(baseSideOutPutProcess).name("jsonParser");
    }
}
