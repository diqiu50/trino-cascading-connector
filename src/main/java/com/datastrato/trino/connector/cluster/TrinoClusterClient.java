/*
 * Copyright 2024 Datastrato
 *
 * Licensed under the Server Side Public License, v 1. You may not use this file
 * except in compliance with the Server Side Public License, v 1.
 */
package com.datastrato.trino.connector.cluster;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class TrinoClusterClient extends BaseJdbcClient {
    private static final Logger log = Logger.get(TrinoClusterClient.class);

    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public TrinoClusterClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier) {
        super(
                "\"",
                connectionFactory,
                queryBuilder,
                config.getJdbcTypesMappedToVarchar(),
                identifierMapping,
                queryModifier,
                true);

        this.connectorExpressionRewriter =
                JdbcConnectorExpressionRewriterBuilder.newBuilder()
                        .addStandardRules(this::quoted)
                        .map("$like(value: varchar, pattern: varchar): boolean")
                        .to("value LIKE pattern")
                        .build();

        JdbcTypeHandle bigintTypeHandle =
                new JdbcTypeHandle(
                        Types.BIGINT,
                        Optional.of("bigint"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());
        this.aggregateFunctionRewriter =
                new AggregateFunctionRewriter<>(
                        this.connectorExpressionRewriter,
                        ImmutableSet
                                .<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>
                                        builder()
                                .add(new ImplementCountAll(bigintTypeHandle))
                                .add(new ImplementCount(bigintTypeHandle))
                                .add(new ImplementMinMax(false))
                                .add(new ImplementSum(TrinoClusterClient::toTypeHandle))
                                .add(new ImplementAvgFloatingPoint())
                                .add(new ImplementAvgDecimal())
                                .add(new ImplementStddevSamp())
                                .add(new ImplementStddevPop())
                                .add(new ImplementVarianceSamp())
                                .add(new ImplementVariancePop())
                                .build());
        log.debug("Create TrinoClusterClient with url: ", config.getConnectionUrl());
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments) {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(
            ConnectorSession session,
            AggregateFunction aggregate,
            Map<String, ColumnHandle> assignments) {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(
            ConnectorSession session,
            JdbcTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType) {
        return Optional.of(
                new JdbcTypeHandle(
                        Types.NUMERIC,
                        Optional.of("decimal"),
                        Optional.of(decimalType.getPrecision()),
                        Optional.of(decimalType.getScale()),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {
        try (ResultSet resultSet =
                connection.getMetaData().getSchemas(connection.getCatalog(), "%")) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected boolean filterSchema(String schemaName) {
        if (schemaName.equalsIgnoreCase("mysql") || schemaName.equalsIgnoreCase("sys")) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    protected void dropSchema(
            ConnectorSession session,
            Connection connection,
            String remoteSchemaName,
            boolean cascade)
            throws SQLException {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support drop schema");
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the driver
            // attempts to drain the connection by reading all the results.
            connection.abort(directExecutor());
        }
    }

    @Override
    public ResultSet getTables(
            Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                connection.getCatalog(),
                schemaName.orElse(null),
                escapeObjectNameForMetadataQuery(tableName, metadata.getSearchStringEscape())
                        .orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    public void setTableComment(
            ConnectorSession session, JdbcTableHandle handle, Optional<String> comment) {
        throw new TrinoException(
                NOT_SUPPORTED, "This connector does not support set table comment");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(
            ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                // Disable pushdown because floating-point values are approximate and not stored as
                // exact values,
                // attempts to treat them as exact in comparisons may lead to problems
                return Optional.of(
                        ColumnMapping.longMapping(
                                REAL,
                                (resultSet, columnIndex) ->
                                        floatToRawIntBits(resultSet.getFloat(columnIndex)),
                                realWriteFunction(),
                                DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits =
                        typeHandle
                                .getDecimalDigits()
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "decimal digits not present"));
                int precision = typeHandle.getRequiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW
                        && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(
                            decimalColumnMapping(
                                    createDecimalType(Decimals.MAX_PRECISION, scale),
                                    getDecimalRoundingMode(session)));
                }
                precision =
                        precision
                                + max(
                                        -decimalDigits,
                                        0); // Map decimal(p, -s) (negative scale) to decimal(p+s,
                // 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(
                        decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                if (typeHandle.getRequiredColumnSize() <= CharType.MAX_LENGTH) {
                    CharType charType = createCharType(typeHandle.getRequiredColumnSize());
                    return Optional.of(
                            ColumnMapping.sliceMapping(
                                    charType, charReadFunction(charType), charWriteFunction()));
                }
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Unsupported char type: length greater than " + CharType.MAX_LENGTH);

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                VarcharType varcharType =
                        typeHandle.getRequiredColumnSize() > VarcharType.MAX_LENGTH
                                ? createUnboundedVarcharType()
                                : createVarcharType(typeHandle.getRequiredColumnSize());
                return Optional.of(
                        ColumnMapping.sliceMapping(
                                varcharType,
                                varcharReadFunction(varcharType),
                                varcharWriteFunction()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(
                        ColumnMapping.sliceMapping(
                                VARBINARY,
                                varbinaryReadFunction(),
                                varbinaryWriteFunction(),
                                FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(
                        ColumnMapping.longMapping(
                                DATE,
                                dateReadFunctionUsingLocalDate(),
                                dateWriteFunctionUsingLocalDate()));

            case Types.TIME:
                TimeType timeType =
                        createTimeType(getTimePrecision(typeHandle.getRequiredColumnSize()));
                requireNonNull(timeType, "timeType is null");
                checkArgument(
                        timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
                return Optional.of(
                        ColumnMapping.longMapping(
                                timeType,
                                timeReadFunction(timeType),
                                timeWriteFunction(timeType.getPrecision())));

            case Types.TIMESTAMP:
                TimestampType timestampType =
                        createTimestampType(
                                getTimestampPrecision(typeHandle.getRequiredColumnSize()));
                return Optional.of(
                        ColumnMapping.longMapping(
                                timestampType,
                                timestampReadFunction(timestampType),
                                timestampWriteFunction(timestampType)));

            case Types.TIMESTAMP_WITH_TIMEZONE:
                TimestampWithTimeZoneType timestampWithTimeZoneType =
                        createTimestampWithTimeZoneType(
                                getTimestampPrecision(typeHandle.getRequiredColumnSize()));
                return Optional.of(
                        ColumnMapping.objectMapping(
                                timestampWithTimeZoneType,
                                longTimestampWithTimeZoneReadFunction(),
                                longTimestampWithTimeZoneWriteFunction()));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction() {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    OffsetDateTime offsetDateTime =
                            resultSet.getObject(columnIndex, OffsetDateTime.class);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            offsetDateTime.toEpochSecond(),
                            (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY);
                });
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction() {
        return (statement, index, value) -> {
            Instant instantValue = Instant.ofEpochMilli(unpackMillisUtc(value));
            statement.setObject(index, instantValue);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction() {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond =
                            (long) floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND)
                                            * NANOSECONDS_PER_MILLISECOND
                                    + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    Instant instantValue = Instant.ofEpochSecond(epochSeconds, nanosOfSecond);
                    statement.setObject(index, instantValue);
                });
    }

    private static int getTimestampPrecision(int timestampColumnSize) {
        if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            return 0;
        }
        int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
        verify(
                1 <= timestampPrecision && timestampPrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION,
                "Unexpected timestamp precision %s calculated from timestamp column size %s",
                timestampPrecision,
                timestampColumnSize);
        return timestampPrecision;
    }

    private static int getTimePrecision(int timeColumnSize) {
        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
            return 0;
        }
        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        verify(
                1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION,
                "Unexpected time precision %s calculated from time column size %s",
                timePrecision,
                timeColumnSize);
        return timePrecision;
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType =
                    format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(
                        format("time(%s)", timeType.getPrecision()),
                        timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(
                    format("time(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION),
                    timeWriteFunction(MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(
                        format("datetime(%s)", timestampType.getPrecision()),
                        timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(
                    format("datetime(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION),
                    longTimestampWriteFunction(timestampType, MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                String dataType = format("timestamp(%d)", timestampWithTimeZoneType.getPrecision());
                if (timestampWithTimeZoneType.getPrecision()
                        <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(
                            dataType, shortTimestampWithTimeZoneWriteFunction());
                }
                return WriteMapping.objectMapping(
                        dataType, longTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(
                    format("timestamp(%d)", MAX_SUPPORTED_DATE_TIME_PRECISION),
                    longTimestampWithTimeZoneWriteFunction());
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping(
                    "char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            if (varcharType.getLength().isPresent()) {
                return WriteMapping.sliceMapping(
                        "varchar(" + varcharType.getLength().get() + ")", varcharWriteFunction());
            } else {
                return WriteMapping.sliceMapping("varchar", varcharWriteFunction());
            }
        }

        throw new TrinoException(
                NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected void renameColumn(
            ConnectorSession session,
            Connection connection,
            RemoteTableName remoteTableName,
            String remoteColumnName,
            String newRemoteColumnName)
            throws SQLException {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support rename column");
    }

    @Override
    public void setColumnType(
            ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type) {
        throw new TrinoException(
                NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName) {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    protected void copyTableSchema(
            ConnectorSession session,
            Connection connection,
            String catalogName,
            String schemaName,
            String tableName,
            String newTableName,
            List<String> columnNames) {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support create as select");
    }

    @Override
    public void renameTable(
            ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName) {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics) {
        return super.implementJoin(
                session,
                joinType,
                leftSource,
                rightSource,
                joinConditions,
                rightAssignments,
                leftAssignments,
                statistics);
    }

    @Override
    protected boolean isSupportedJoinCondition(
            ConnectorSession session, JdbcJoinCondition joinCondition) {
        return joinCondition.getOperator() != JoinCondition.Operator.IS_DISTINCT_FROM;
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle) {
        return TableStatistics.empty();
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction() {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    public boolean supportsTopN(
            ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder) {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction() {
        return Optional.of(
                (query, sortItems, limit) -> {
                    String orderBy =
                            sortItems.stream()
                                    .flatMap(
                                            sortItem -> {
                                                String ordering =
                                                        sortItem.getSortOrder().isAscending()
                                                                ? "ASC"
                                                                : "DESC";
                                                String columnSorting =
                                                        format(
                                                                "%s %s",
                                                                quoted(
                                                                        sortItem.getColumn()
                                                                                .getColumnName()),
                                                                ordering);

                                                switch (sortItem.getSortOrder()) {
                                                    case ASC_NULLS_FIRST:
                                                        return Stream.of(
                                                                format(
                                                                        "%s ASC NULLS FIRST",
                                                                        quoted(
                                                                                sortItem.getColumn()
                                                                                        .getColumnName())),
                                                                columnSorting);

                                                    case DESC_NULLS_LAST:
                                                        return Stream.of(
                                                                format(
                                                                        "%s DESC NULLS LAST",
                                                                        quoted(
                                                                                sortItem.getColumn()
                                                                                        .getColumnName())),
                                                                columnSorting);

                                                    case ASC_NULLS_LAST:
                                                    case DESC_NULLS_FIRST:
                                                        return Stream.of(columnSorting);
                                                }
                                                throw new UnsupportedOperationException(
                                                        "Unsupported sort order: "
                                                                + sortItem.getSortOrder());
                                            })
                                    .collect(joining(", "));
                    return format("%s ORDER BY %s LIMIT %s", query, orderBy, limit);
                });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session) {
        return true;
    }
}
