/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */
package com.datastrato.trino.connector.cluster;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class TrinoClusterClientModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(JdbcClient.class)
                .annotatedWith(ForBaseJdbc.class)
                .to(TrinoClusterClient.class)
                .in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TrinoClusterJdbcConfig.class);
        configBinder(binder).bindConfig(TrinoClusterConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class)
                .addBinding()
                .toProvider(Query.class)
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            TrinoClusterConfig trinoClusterConfig,
            OpenTelemetry openTelemetry)
            throws SQLException {
        return new DriverConnectionFactory(
                new TrinoDriver(),
                config.getConnectionUrl(),
                getConnectionProperties(trinoClusterConfig),
                credentialProvider,
                openTelemetry);
    }

    public static Properties getConnectionProperties(TrinoClusterConfig trinoClusterConfig) {
        return new Properties();
    }
}
