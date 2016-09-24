package com.spotify.heroic.metric.bigtable.api;

import com.google.protobuf.ByteString;
import eu.toolchain.async.AsyncFuture;

public interface BigtableMutator {
    AsyncFuture<Void> mutateRow(String tableName, ByteString rowKey, Mutations mutations);

    void close();
}
