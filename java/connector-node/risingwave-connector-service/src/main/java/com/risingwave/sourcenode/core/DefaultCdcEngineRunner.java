package com.risingwave.sourcenode.core;

import com.risingwave.connector.api.source.*;
import com.risingwave.proto.ConnectorServiceProto;
import io.debezium.engine.DebeziumEngine;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single-thread engine runner */
public class DefaultCdcEngineRunner implements CdcEngineRunner {
    static final Logger LOG = LoggerFactory.getLogger(DefaultCdcEngineRunner.class);

    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CdcEngine engine;

    public DefaultCdcEngineRunner(CdcEngine engine) {
        this.executor = Executors.newSingleThreadExecutor();
        this.engine = engine;
    }

    public static CdcEngineRunner newCdcEngineRunner(
            long sourceId,
            SourceConfig config,
            StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver) {
        DefaultCdcEngineRunner runner = null;
        try {
            var engine =
                    new DefaultCdcEngine(
                            config,
                            new DebeziumEngine.CompletionCallback() {
                                @Override
                                public void handle(
                                        boolean success, String message, Throwable error) {
                                    if (!success) {
                                        responseObserver.onError(error);
                                        LOG.error(
                                                "failed to run the engine. message: {}",
                                                message,
                                                error);
                                    } else {
                                        responseObserver.onCompleted();
                                    }
                                }
                            });

            runner = new DefaultCdcEngineRunner(engine);
        } catch (Exception e) {
            LOG.error("failed to create the CDC engine", e);
        }
        return runner;
    }

    /** Start to run the cdc engine */
    public void start() {
        if (isRunning()) {
            LOG.info("CdcEngine#{} already started", engine.getId());
            return;
        }

        executor.execute(engine);
        running.set(true);
        LOG.info("CdcEngine#{} started", engine.getId());
    }

    public void stop() throws Exception {
        if (isRunning()) {
            engine.stop();
            cleanUp();
            LOG.info("CdcEngine#{} terminated", engine.getId());
        }
    }

    @Override
    public CdcEngine getEngine() {
        return engine;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void cleanUp() {
        running.set(false);
        executor.shutdownNow();
    }
}
