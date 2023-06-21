package com.lake.process;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lake.bean.TableMapperBean;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


    static class JsonParserProcess extends ProcessFunction<String, RowData> {

        private List<TableMapperBean> tableMapperBeanList;
        private String namespace;
        private CatalogLoader catalogLoader;
        private static Map<String, OutputTag<RowData>> outputTagMap;
        private static Map<String, JsonRowDataDeserializationSchema> jsonRowDataDeserializationSchemaMap;
        private static Map<String, String> tableIdentMap;

        public JsonParserProcess() {
        }

        public JsonParserProcess(List<TableMapperBean> tableMapperBeanList, String namespace, CatalogLoader catalogLoader) {
            this.tableMapperBeanList = tableMapperBeanList;
            this.namespace = namespace;
            this.catalogLoader = catalogLoader;
        }

        @Override
        public void processElement(String input, Context context, Collector<RowData> collector) throws Exception {
            if (input != null) {
                JSONObject jsonObject = JSON.parseObject(input);
                String op = jsonObject.getString("datatype");
                String sourceTableName = jsonObject.getString("itfcode").toLowerCase();
                //获取目标端表名
                String sinkTableName = tableIdentMap.get(sourceTableName);
                if (sinkTableName == null || sinkTableName.isEmpty()) {
                    return;
                }
                    if (op.equals("I")) {
                        RowData rowData = getRowData(jsonObject, "dataload", sinkTableName);
                        rowData.setRowKind(RowKind.INSERT);
                        context.output(outputTagMap.get(sinkTableName), rowData);
                    } else if (op.equals("U")) {
                        RowData rowData1 = getRowData(jsonObject, "BEFORE", sinkTableName);
                        rowData1.setRowKind(RowKind.UPDATE_BEFORE);
                        RowData rowData2 = getRowData(jsonObject, "dataload", sinkTableName);
                        rowData2.setRowKind(RowKind.UPDATE_AFTER);
                        context.output(outputTagMap.get(sinkTableName), rowData1);
                        context.output(outputTagMap.get(sinkTableName), rowData2);
                    } else if (op.equals("D")) {
                        RowData rowData = getRowData(jsonObject, "BEFORE", sinkTableName);
                        rowData.setRowKind(RowKind.DELETE);
                        context.output(outputTagMap.get(sinkTableName), rowData);
                    }
            }
        }

        private RowData getRowData(JSONObject jsonObject, String mode, String sinkTableName) throws Exception {
            RowData rowData;
            JsonRowDataDeserializationSchema jrd = jsonRowDataDeserializationSchemaMap.get(sinkTableName);
            rowData = jrd.deserialize(jsonObject.getString(mode).getBytes(StandardCharsets.UTF_8));
            return rowData;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            generateOutputTagAndDeserialization();
        }

        private void generateOutputTagAndDeserialization() {
            outputTagMap = new HashMap<>();
            jsonRowDataDeserializationSchemaMap = new HashMap<>();
            tableIdentMap = new HashMap<>();
            tableMapperBeanList.forEach(x -> {
                String sinkTableName = x.getSinkTableName();
                outputTagMap.put(sinkTableName, new OutputTag<RowData>(x.getSinkTableName()) {
                });

                // json-RowData 序列化
                TableIdentifier tableIdentifier = TableIdentifier.of(namespace, sinkTableName);
                Table table = catalogLoader.loadCatalog().loadTable(tableIdentifier);

                RowType rowType = FlinkSchemaUtil.convert(table.schema());
                JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                        new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                                false, false, TimestampFormat.SQL);
                jsonRowDataDeserializationSchemaMap.put(sinkTableName, jsonRowDataDeserializationSchema);

                tableIdentMap.put(x.getSourceTableName(), sinkTableName);
            });
        }
    }
}
