package com.lake.catalog.impl;

import com.lake.catalog.CatalogInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;

import javax.annotation.Nullable;

import java.net.MalformedURLException;

public class HadoopCatalogInitializer implements CatalogInitializer {

    private String warehouse;
    private Configuration conf;

    @Override
    public HadoopCatalog getCatalog() throws MalformedURLException {
        HadoopCatalog catalog = new HadoopCatalog(conf, warehouse);
        return catalog;
    }

    @Override
    public HadoopCatalogInitializer setConfig(String warehouse, @Nullable String uri, Configuration configuration) throws Exception {
        conf = configuration;
        this.warehouse = warehouse;
        return this;
    }

    public HadoopCatalog getCatalogByConfig(Configuration config) {
        HadoopCatalog catalog = new HadoopCatalog(config, warehouse);
        return catalog;
    }

    public Configuration getConf() {
        return conf;
    }
}
