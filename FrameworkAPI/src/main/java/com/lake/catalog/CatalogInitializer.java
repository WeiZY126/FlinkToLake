package com.lake.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;

import javax.annotation.Nullable;

import java.net.MalformedURLException;

public interface CatalogInitializer {
    public BaseMetastoreCatalog getCatalog() throws MalformedURLException;

    public CatalogInitializer setConfig(String warehouseUri, @Nullable String uri, Configuration configuration) throws Exception;
}
