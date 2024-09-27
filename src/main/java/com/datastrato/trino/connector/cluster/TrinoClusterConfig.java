/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */
package com.datastrato.trino.connector.cluster;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;

import java.util.concurrent.TimeUnit;

public class TrinoClusterConfig {
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    @Config("trino.auto-reconnect")
    public TrinoClusterConfig setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects() {
        return maxReconnects;
    }

    @Config("trino.max-reconnects")
    public TrinoClusterConfig setMaxReconnects(int maxReconnects) {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    @Config("trino.connection-timeout")
    public TrinoClusterConfig setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }
}
