/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */

package com.datastrato.trino.connector.cluster;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TestingTrinoServer implements Closeable {
    private static final DockerImageName TRINO_IMAGE =
            DockerImageName.parse("trinodb/trino").withTag("435");

    private final TrinoContainer dockerContainer;
    private final Connection connection;

    public TestingTrinoServer() {
        this(TRINO_IMAGE);
    }

    public TestingTrinoServer(DockerImageName image) {
        dockerContainer = new TrinoContainer(image).withStartupAttempts(3).withExposedPorts(8080);
        dockerContainer.withCreateContainerCmdModifier(
                createContainerCmd ->
                        new HostConfig()
                                .withPortBindings(
                                        new PortBinding(
                                                Ports.Binding.bindPort(8080),
                                                new ExposedPort(8080))));
        dockerContainer.withUsername("admin");
        dockerContainer.start();
        connection = createConnection();
    }

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(
                    dockerContainer.getJdbcUrl(),
                    dockerContainer.getUsername(),
                    dockerContainer.getPassword());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create connection", e);
        }
    }

    public void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl() {
        return String.format(
                "jdbc:trino://%s:%s",
                dockerContainer.getHost(), dockerContainer.getMappedPort(8080));
    }

    @Override
    public void close() {
        dockerContainer.stop();
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close connection", e);
            }
        }
    }

    @ResourcePresence
    public boolean isRunning() {
        return dockerContainer.getContainerId() != null;
    }
}
