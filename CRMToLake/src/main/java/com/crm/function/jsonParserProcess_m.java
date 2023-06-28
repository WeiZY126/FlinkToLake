package com.crm.function;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lake.Exceptions.TableMappingRelationshipNotFoundException;
import com.lake.bean.TableMapperBean;
import com.lake.process.JsonSideOutPutProcess;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.List;

public class jsonParserProcess_m extends JsonSideOutPutProcess<String> {
    public jsonParserProcess_m() {
    }

    public jsonParserProcess_m(List<TableMapperBean> tableMapperBeanList, String namespace, CatalogLoader catalogLoader) {
        super(tableMapperBeanList, namespace, catalogLoader);
    }

    @Override
    public void processElement(String input, Context context, Collector<RowData> collector) throws Exception {
        if (input != null) {
            JSONObject jsonObject = JSON.parseObject(input);
            String op = jsonObject.getString("datatype");
            String sourceTableName = jsonObject.getString("itfcode").toLowerCase();
            //获取目标端表名
            String sinkTableName;
            try {
                sinkTableName = getSinkTableName(sourceTableName);
            } catch (TableMappingRelationshipNotFoundException exception) {
                //doNoting...
                return;
            }

            if (op.equals("I")) {
                RowData rowData = getRowDataFromJson(jsonObject.getString("dataload").replaceAll(":null,",""), sinkTableName);
                rowData.setRowKind(RowKind.INSERT);
                context.output(outputTagMap.get(sinkTableName), rowData);
            } else if (op.equals("U")) {
                RowData rowData1 = getRowDataFromJson(jsonObject.getString("BEFORE"), sinkTableName);
                rowData1.setRowKind(RowKind.UPDATE_BEFORE);
                RowData rowData2 = getRowDataFromJson(jsonObject.getString("dataload"), sinkTableName);
                rowData2.setRowKind(RowKind.UPDATE_AFTER);
                context.output(outputTagMap.get(sinkTableName), rowData1);
                context.output(outputTagMap.get(sinkTableName), rowData2);
            } else if (op.equals("D")) {
                RowData rowData = getRowDataFromJson(jsonObject.getString("BEFORE"), sinkTableName);
                rowData.setRowKind(RowKind.DELETE);
                context.output(outputTagMap.get(sinkTableName), rowData);
            }
        }
    }
}
