/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */
package com.datastrato.trino.connector.cluster;

import io.trino.jdbc.TrinoDriver;
import io.trino.jdbc.TrinoDriverUri;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;

import java.sql.SQLException;
import java.util.Properties;

public class TrinoClusterJdbcConfig extends BaseJdbcConfig {
    @AssertTrue(message = "Invalid JDBC URL for trino cluster cascading connector")
    public boolean isUrlValid() {
        try (TrinoDriver driver = new TrinoDriver()) {
            return driver.acceptsURL(getConnectionUrl());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AssertTrue(message = "Catalog must be specified in JDBC URL for trino cascading connector")
    public boolean isUrlWithCatalog() {
        try {
            TrinoDriverUri trinoDriverUri =
                    TrinoDriverUri.create(getConnectionUrl(), new Properties());
            return trinoDriverUri.getCatalog().isPresent();
        } catch (SQLException ignore) {
            return false;
        }
    }
}
