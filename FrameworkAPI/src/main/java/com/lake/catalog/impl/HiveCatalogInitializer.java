package com.lake.catalog.impl;

import com.lake.catalog.CatalogInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;
import java.util.Map;

public class HiveCatalogInitializer implements CatalogInitializer {
    private static Map<String, String> hiveConfigMap;
    private Configuration conf;

    @Override
    public HiveCatalog getCatalog() {
        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(conf);
        catalog.initialize("hive", hiveConfigMap);
        return catalog;
    }

    @Override
    public HiveCatalogInitializer setConfig(String warehouse, String uri, Configuration configuration) throws Exception {
        hiveConfigMap = new HashMap<>();
        conf = configuration;
        hiveConfigMap.put("uri", uri);
        hiveConfigMap.put("warehouse", warehouse);
        return this;
    }
}
