package com.lake.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import com.lake.catalog.impl.HadoopCatalogInitializer;
import com.lake.catalog.impl.HiveCatalogInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.HashMap;
import java.util.Map;

public class IcebergUtil {

    public static BaseMetastoreCatalog initCatalog(ParameterTool parameterTool, Configuration configuration) throws Exception {
        return initCatalog(parameterTool.get("iceberg.catalog.type"),
                parameterTool.get("iceberg.catalog.warehouse"),
                parameterTool.has("iceberg.catalog.uri") ? parameterTool.get("iceberg.catalog.uri") : null,
                configuration);
    }

    public static BaseMetastoreCatalog initCatalog(String catalogType, String catalogWareHouse, String catalogURI, Configuration configuration) throws Exception {
        if ("Hadoop Catalog".equals(catalogType)) {
            return new HadoopCatalogInitializer().setConfig(catalogWareHouse, "", configuration).getCatalog();
        } else if ("Hive Catalog".equals(catalogType)) {
            return new HiveCatalogInitializer().setConfig(catalogWareHouse, catalogURI, configuration).getCatalog();
        } else {
            throw new Exception("unknow catalog type");
        }
    }

    public static CatalogLoader getCatalogLoader(String catalogType, String catalogWareHouse, String catalogURI, Configuration configuration) throws Exception {
        Map<String,String> ConfigMap = new HashMap<>();
        if ("Hadoop Catalog".equals(catalogType)) {
            ConfigMap.put("warehouse", catalogWareHouse);
            return CatalogLoader.hadoop("hadoop_catalog",configuration,ConfigMap);
        } else if ("Hive Catalog".equals(catalogType)) {
            ConfigMap.put("uri", catalogURI);
            ConfigMap.put("warehouse", catalogWareHouse);
            return CatalogLoader.hive("hive_catalog",configuration,ConfigMap);
        } else {
            throw new Exception("unknow catalog type");
        }
    }

    public static CatalogLoader getCatalogLoader(ParameterTool parameterTool, Configuration configuration) throws Exception {
        return getCatalogLoader(parameterTool.get("iceberg.catalog.type"),
                parameterTool.get("iceberg.catalog.warehouse"),
                parameterTool.has("iceberg.catalog.uri") ? parameterTool.get("iceberg.catalog.uri") : null,
                configuration);
    }
}
