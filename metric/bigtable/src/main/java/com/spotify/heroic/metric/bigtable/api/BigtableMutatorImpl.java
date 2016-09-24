package com.spotify.heroic.metric.bigtable.api;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BigtableMutatorImpl implements BigtableMutator {
    private final AsyncFramework async;
    private final com.google.cloud.bigtable.grpc.BigtableSession session;
    private final boolean disableBulkMutations;
    private final Map<String, BulkMutation> tableToBulkMutation;
    private final ScheduledExecutorService scheduler;

    public BigtableMutatorImpl(
        AsyncFramework async,
        com.google.cloud.bigtable.grpc.BigtableSession session,
        boolean disableBulkMutations,
        int flushIntervalSeconds)
    {
        this.async = async;
        this.session = session;
        this.disableBulkMutations = disableBulkMutations;

        if (!disableBulkMutations) {
            this.scheduler = null;
            this.tableToBulkMutation = null;
        } else {
            this.tableToBulkMutation = new HashMap<>();
            this.scheduler = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("heroic-bigtable-flush").build());
            scheduler.scheduleAtFixedRate(this::flush, 0, flushIntervalSeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public AsyncFuture<Void> mutateRow(
        String tableName, ByteString rowKey, Mutations mutations)
    {
        if (disableBulkMutations) {
            return mutateSingleRow(tableName, rowKey, mutations);
        } else {
            return mutateBatchRow(tableName, rowKey, mutations);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();

        try {
            scheduler.awaitTermination(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            log.error("Failed to shut down bigtable flush executor service in a timely manner");
        }
    }

    private AsyncFuture<Void> mutateSingleRow(
        String tableName, ByteString rowKey, Mutations mutations
    ) {
        return convertVoid(
            session
                .getDataClient()
                .mutateRowAsync(toMutateRowRequest(tableName, rowKey, mutations)));
    }

    private AsyncFuture<Void> mutateBatchRow(
        String tableName, ByteString rowKey, Mutations mutations)
    {
        final BulkMutation bulkMutation = getOrAddBulkMutation(tableName);
        return convertVoid(bulkMutation.add(toMutateRowRequest(tableName, rowKey, mutations)));
    }

    private synchronized BulkMutation getOrAddBulkMutation(String tableName) {
        if (tableToBulkMutation.containsKey(tableName)) {
            return tableToBulkMutation.get(tableName);
        }

        final BulkMutation bulkMutation = session
            .createBulkMutation(
                session
                    .getOptions()
                    .getInstanceName()
                    .toTableName(tableName),
              session.createAsyncExecutor());

        tableToBulkMutation.put(tableName, bulkMutation);

        return bulkMutation;
    }

    private MutateRowRequest toMutateRowRequest(
        String tableName,
        ByteString rowKey,
        Mutations mutations)
    {
        return MutateRowRequest
            .newBuilder()
            .setTableName(session.getOptions().getInstanceName().toTableNameStr(tableName))
            .setRowKey(rowKey)
            .addAllMutations(mutations.getMutations())
            .build();
    }

    private <T> AsyncFuture<Void> convertVoid(final ListenableFuture<T> request) {
        final ResolvableFuture<Void> future = async.future();

        Futures.addCallback(request, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                future.resolve(null);
            }

            @Override
            public void onFailure(Throwable t) {
                future.fail(t);
            }
        });

        return future;
    }

    private synchronized void flush() {
        tableToBulkMutation.values().stream().forEach(BulkMutation::flush);
    }
}
