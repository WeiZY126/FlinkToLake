package com.lake.bean;

import java.io.Serializable;
import java.util.Objects;

/**
 * 表映射实体类
 */
public class TableMapperBean implements Serializable {

    //源端表名(识别名)
    private String sourceTableName;
    //目标端表名
    private String sinkTableName;
    //slot组
    private String slotSharingGroupName;

    public TableMapperBean() {
    }

    public TableMapperBean(String sourceTableName, String sinkTableName, String slotSharingGroupName) {
        this.sourceTableName = sourceTableName;
        this.sinkTableName = sinkTableName;
        this.slotSharingGroupName = slotSharingGroupName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getSinkTableName() {
        return sinkTableName;
    }

    public void setSinkTableName(String sinkTableName) {
        this.sinkTableName = sinkTableName;
    }

    public String getSlotSharingGroupName() {
        return slotSharingGroupName;
    }

    public void setSlotSharingGroupName(String slotSharingGroupName) {
        this.slotSharingGroupName = slotSharingGroupName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableMapperBean that = (TableMapperBean) o;
        return sourceTableName.equals(that.sourceTableName) && sinkTableName.equals(that.sinkTableName) && Objects.equals(slotSharingGroupName, that.slotSharingGroupName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTableName, sinkTableName, slotSharingGroupName);
    }
}
