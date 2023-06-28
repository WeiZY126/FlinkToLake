package com.lake.process;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;

import com.lake.Exceptions.TableMappingRelationshipNotFoundException;
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

    /**
     * 通过json串和sinkTableName，获取对应的RowData
     *
     * @param jsonString
     * @param sinkTableName
     * @return
     * @throws Exception
     */
    protected RowData getRowDataFromJson(String jsonString, String sinkTableName) throws Exception {
        JsonRowDataDeserializationSchema jrd = jsonRowDataDeserializationSchemaMap.get(sinkTableName);
        if (jrd == null) {
            throw new TableMappingRelationshipNotFoundException("未找到表" + sinkTableName + "映射关系，请检查是否存在");
        }
        RowData rowData = jrd.deserialize(jsonString.getBytes(StandardCharsets.UTF_8));
        return rowData;
    }

    /**
     * 初始化方法
     */
    @Override
    protected void generateOutputTagAndDeserialization() {
        outputTagMap = new HashMap<>();
        jsonRowDataDeserializationSchemaMap = new HashMap<>();
        tableIdentMap = new HashMap<>();
        tableMapperBeanList.forEach(tableMapperBean -> {
            String sinkTableName = tableMapperBean.getSinkTableName();

            //初始化侧输出流与sinkTableName映射关系
            outputTagMap.put(sinkTableName, new OutputTag<RowData>(tableMapperBean.getSinkTableName()) {
            });

            //初始化序列化器与sinkTableName映射关系
            TableIdentifier tableIdentifier = TableIdentifier.of(namespace, sinkTableName);
            Table table = catalogLoader.loadCatalog().loadTable(tableIdentifier);

            RowType rowType = FlinkSchemaUtil.convert(table.schema());
            JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                    new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                            false, false, TimestampFormat.SQL);
            jsonRowDataDeserializationSchemaMap.put(sinkTableName, jsonRowDataDeserializationSchema);

            //初始化sourceTableName与sinkTableName映射关系
            tableIdentMap.put(tableMapperBean.getSourceTableName(), sinkTableName);
        });
    }
}
