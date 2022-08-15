package com.dulio.demo.raft.tsdb.model;

import java.io.Serializable;
import java.util.Date;

/**
 * @class: TsdbMetaValue
 * @program: raft-demo
 * @description: 该版本不做数据分片，只做数据副本
 *  TODO 下一步可以支持分片
 * @author: huadu.shen
 * @created: 2021/06/20 22:06
 */
public class TsdbMetaValue implements Serializable {
    private String[] quorumReplicas;
    private int quorumWrite;
    private int quorumRead;
}
