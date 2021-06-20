package com.dulio.demo.raft.tsdb.model;

import java.io.Serializable;
import java.util.Date;

/**
 * @class: TsdbMetaKey
 * @program: raft-demo
 * @description: Tsdb meta data
 * @author: huadu.shen
 * @created: 2021/06/20 22:06
 */
public class TsdbMetaKey implements Serializable {
    private String databaseName;
    private String measurement;
    private String retentionPolicy;
    private Date startDatetime;
    private Date endDatetime;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = result + databaseName.hashCode() * prime;
        result = result + measurement.hashCode() * prime;
        result = result + retentionPolicy.hashCode() * prime;
        result = result + startDatetime.hashCode() * prime;
        result = result + endDatetime.hashCode() * prime;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TsdbMetaKey other = (TsdbMetaKey) obj;
        if (!databaseName.equals(other.databaseName)) return false;
        if (!measurement.equals(other.measurement)) return false;
        if (!retentionPolicy.equals(other.retentionPolicy)) return false;
        if (!startDatetime.equals(other.startDatetime)) return false;
        if (!endDatetime.equals(other.endDatetime)) return false;
        return true;
    }
}
