package com.lake.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

import com.lake.Exceptions.TableMappingRelationshipNotFoundException;
import com.lake.bean.TableMapperBean;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.List;
import java.util.Map;

public abstract class BaseSideOutPutProcess<T> extends ProcessFunction<T, RowData> {
    protected List<TableMapperBean> tableMapperBeanList;
    protected String namespace;
    protected CatalogLoader catalogLoader;
    protected Map<String, OutputTag<RowData>> outputTagMap;
    protected Map<String, String> tableIdentMap;

    protected BaseSideOutPutProcess() {
    }

    protected BaseSideOutPutProcess(List<TableMapperBean> tableMapperBeanList, String namespace, CatalogLoader catalogLoader) {
        this.tableMapperBeanList = tableMapperBeanList;
        this.namespace = namespace;
        this.catalogLoader = catalogLoader;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        generateOutputTagAndDeserialization();
    }

    /**
     * 通过sourceTableName获取sinkTableName
     * @param sourceTableName
     * @return
     * @throws Exception
     */
    protected String getSinkTableName(String sourceTableName) throws Exception {
        String sinkTableName = tableIdentMap.get(sourceTableName);
        if (sinkTableName == null || sinkTableName.isEmpty()) {
            throw new TableMappingRelationshipNotFoundException("未找到表映射关系");
        }
        return sinkTableName;
    }

    protected abstract void generateOutputTagAndDeserialization();
}
