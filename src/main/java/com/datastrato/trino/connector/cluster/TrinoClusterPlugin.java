/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */
package com.datastrato.trino.connector.cluster;

import io.trino.plugin.jdbc.JdbcPlugin;

public class TrinoClusterPlugin extends JdbcPlugin {
    public TrinoClusterPlugin() {
        super("trino", new TrinoClusterClientModule());
    }
}
