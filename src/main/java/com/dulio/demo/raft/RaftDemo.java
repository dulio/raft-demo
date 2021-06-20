package com.dulio.demo.raft;

import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.closure.TaskClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.codahale.metrics.ConsoleReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class RaftDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftDemo.class);

    public static void main(String[] args) {
//        Endpoint addr = JRaftUtils.getEndPoint("localhost:8080");
//        PeerId peer = JRaftUtils.getPeerId("localhost:8080");
//        // 三个节点组成的 raft group 配置，注意节点之间用逗号隔开
//        Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");

        String groupId = "jraft";
        PeerId serverId = JRaftUtils.getPeerId("localhost:8080");
        NodeOptions nodeOptions = new NodeOptions();  // 配置 node options
        nodeOptions.setEnableMetrics(true);

        RaftGroupService cluster = new RaftGroupService(groupId, serverId, nodeOptions);
        Node node = cluster.start();

        // 使用 node 提交任务
        Closure done = new TaskClosure() {
            @Override
            public void onCommitted() {
                LOGGER.info("Task Closure committed");
            }

            @Override
            public void run(Status status) {
                LOGGER.info("status={}", status);
            }
        };
        Task task = new Task();
        task.setData(ByteBuffer.wrap("hello".getBytes()));
        task.setDone(done);
        node.apply(task);

        // 将指标定期 30 秒间隔输出到 console
        ConsoleReporter reporter = ConsoleReporter.forRegistry(node.getNodeMetrics().getMetricRegistry())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(30, TimeUnit.SECONDS);
    }
}
