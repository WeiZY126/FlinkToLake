package com.lake.process;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;

import com.lake.bean.TableMapperBean;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class JsonSideOutPutProcess<T> extends BaseSideOutPutProcess<T> {

    protected static Map<String, JsonRowDataDeserializationSchema> jsonRowDataDeserializationSchemaMap;

    protected JsonSideOutPutProcess() {
        super();
    }

    protected JsonSideOutPutProcess(List<TableMapperBean> tableMapperBeanList, String namespace, CatalogLoader catalogLoader) {
        super(tableMapperBeanList, namespace, catalogLoader);
    }

    protected RowData getRowDataFromJson(String jsonString, String sinkTableName) throws Exception {
        RowData rowData;
        JsonRowDataDeserializationSchema jrd = jsonRowDataDeserializationSchemaMap.get(sinkTableName);
        rowData = jrd.deserialize(jsonString.getBytes(StandardCharsets.UTF_8));
        return rowData;
    }

    @Override
    protected void generateOutputTagAndDeserialization() {
        outputTagMap = new HashMap<>();
        jsonRowDataDeserializationSchemaMap = new HashMap<>();
        tableIdentMap = new HashMap<>();
        tableMapperBeanList.forEach(tableMapperBean -> {
            String sinkTableName = tableMapperBean.getSinkTableName();
            outputTagMap.put(sinkTableName, new OutputTag<RowData>(tableMapperBean.getSinkTableName()) {
            });

            // json-RowData 序列化
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, sinkTableName);
            Table table = catalogLoader.loadCatalog().loadTable(tableIdentifier);

            RowType rowType = FlinkSchemaUtil.convert(table.schema());
            JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                    new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                            false, false, TimestampFormat.SQL);
            jsonRowDataDeserializationSchemaMap.put(sinkTableName, jsonRowDataDeserializationSchema);

            tableIdentMap.put(tableMapperBean.getSourceTableName(), sinkTableName);
        });
    }
}
