package com.lake.sink;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

import com.lake.bean.TableMapperBean;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.sink.FlinkSinkWithSlotGroup;

import java.util.List;

public class IcebergSinkBuilder {
    private List<TableMapperBean> tableMapperBeanList;
    private SingleOutputStreamOperator inputStream;
    private ParameterTool parameterTool;

    private IcebergSinkBuilder(List<TableMapperBean> tableMapperBeanList, SingleOutputStreamOperator inputStream, ParameterTool parameterTool) {
        this.tableMapperBeanList = tableMapperBeanList;
        this.inputStream = inputStream;
        this.parameterTool = parameterTool;
    }

    public static IcebergSinkBuilder of(List<TableMapperBean> tableMapperBeanList, SingleOutputStreamOperator inputStream, ParameterTool parameterTool){
        return new IcebergSinkBuilder(tableMapperBeanList,inputStream,parameterTool);
    }
    private IcebergSinkBuilder() {
    }

    public void build(CatalogLoader catalogLoader) throws Exception {
        //拼接侧输出流与icebergSink
        tableMapperBeanList.forEach(tableMapperBean -> {
            String sinkTableName = tableMapperBean.getSinkTableName();

            //获取侧输出流
            DataStream sideOutput = inputStream.getSideOutput(new OutputTag<RowData>(sinkTableName) {
            });

            //获取tableLoader
            TableIdentifier tableIdentifier = TableIdentifier.of(parameterTool.get("iceberg.namespace.name"), sinkTableName);
            TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

//            FlinkSinkWithSlotGroup.Builder builder = FlinkSinkWithSlotGroup
//                    .forRowData(sideOutput)
//                    .upsert(true)
//                    .distributionMode(DistributionMode.HASH)
//                    .tableLoader(tableLoader)
//                    .writeParallelism(1);
//            if (tableMapperBean.getSlotSharingGroupName() != null) {
//                builder.slotSharingGroup(SlotSharingGroup.newBuilder(tableMapperBean.getSlotSharingGroupName()).build());
//            }
//            builder.append();

            FlinkSink
                    .forRowData(sideOutput)
                    .upsert(true)
                    .distributionMode(DistributionMode.HASH)
                    .tableLoader(tableLoader)
                    .writeParallelism(1)
                    .append();
        });
    }


}

