package com.dulio.demo.raft.tsdb;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import com.dulio.demo.raft.counter.CounterClosure;
import com.dulio.demo.raft.counter.CounterOperation;
import com.dulio.demo.raft.tsdb.model.TsdbMetaKey;
import com.dulio.demo.raft.tsdb.model.TsdbMetaValue;
import com.dulio.demo.raft.tsdb.snapshot.TsdbMetaSnapshotFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class TsdbMetaStateMachine extends StateMachineAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TsdbMetaStateMachine.class);

    /**
     * TsdbMetaMap
     */
    private final ConcurrentMap<TsdbMetaKey, TsdbMetaValue> tsdbMetaData = new ConcurrentHashMap<>();
    /**
     * Leader term
     */
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            long current = 0;
            CounterOperation counterOperation = null;

            CounterClosure closure = null;
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (CounterClosure) iter.done();
                counterOperation = closure.getCounterOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    counterOperation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                            data.array(), CounterOperation.class.getName());
                } catch (final CodecException e) {
                    LOGGER.error("Fail to decode IncrementAndGetRequest", e);
                }
            }
//            if (counterOperation != null) {
//                switch (counterOperation.getOp()) {
//                    case GET:
//                        current = this.value.get();
//                        LOGGER.info("Get value={} at logIndex={}", current, iter.getIndex());
//                        break;
//                    case INCREMENT:
//                        final long delta = counterOperation.getDelta();
//                        final long prev = this.value.get();
//                        current = this.value.addAndGet(delta);
//                        LOGGER.info("Added value={} by delta={} at logIndex={}", prev, delta, iter.getIndex());
//                        break;
//                }
//
//                if (closure != null) {
//                    closure.success(current);
//                    closure.run(Status.OK());
//                }
//            }
            iter.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        final ConcurrentMap<TsdbMetaKey, TsdbMetaValue> currVal = this.tsdbMetaData;
        Utils.runInThread(() -> {
            final TsdbMetaSnapshotFile snapshot = new TsdbMetaSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(currVal)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public void onError(final RaftException e) {
        LOGGER.error("Raft error: {}", e, e);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOGGER.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOGGER.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final TsdbMetaSnapshotFile snapshot = new TsdbMetaSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            this.tsdbMetaData.clear();
            this.tsdbMetaData.putAll(snapshot.load());
            return true;
        } catch (final IOException e) {
            LOGGER.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

}
