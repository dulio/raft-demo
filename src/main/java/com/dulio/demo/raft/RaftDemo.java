package com.dulio.demo.raft;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;

public class RaftDemo {
    public static void main(String[] args) {
        Endpoint addr = JRaftUtils.getEndPoint("localhost:8080");
        PeerId peer = JRaftUtils.getPeerId("localhost:8080");
        // 三个节点组成的 raft group 配置，注意节点之间用逗号隔开
        Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");
    }
}
