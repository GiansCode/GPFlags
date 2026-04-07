package me.ryanhamshire.GPFlags;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * Manages a MySQL connection pool and provides all CRUD operations for GPFlags data.
 * Uses HikariCP for efficient connection pooling.
 */
public class DatabaseManager {

    private HikariDataSource dataSource;
    private final GPFlags plugin;
    private final String flagsTable;

    public DatabaseManager(@NotNull GPFlags plugin,
                           @NotNull String host,
                           int port,
                           @NotNull String database,
                           @NotNull String username,
                           @NotNull String password,
                           @NotNull String tablePrefix) {
        this.plugin = plugin;
        this.flagsTable = tablePrefix + "flags";

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database
                + "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8mb4"
                + "&autoReconnect=true&serverTimezone=UTC");
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setPoolName("GPFlags-Pool");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30_000);
        config.setIdleTimeout(600_000);
        config.setMaxLifetime(1_800_000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");

        try {
            this.dataSource = new HikariDataSource(config);
            createTables();
            plugin.getLogger().info("MySQL connection established successfully.");
        } catch (Exception e) {
            plugin.getLogger().log(Level.SEVERE,
                    "Failed to connect to MySQL. Falling back to YAML storage.", e);
            if (this.dataSource != null) {
                this.dataSource.close();
            }
            this.dataSource = null;
        }
    }

    /**
     * Returns true if the connection pool is initialised and open.
     */
    public boolean isConnected() {
        return dataSource != null && !dataSource.isClosed();
    }

    private void createTables() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS `" + flagsTable + "` ("
                    + "  `scope_id`  VARCHAR(255) NOT NULL COMMENT 'Claim ID, world name, -2 (default), or ''everywhere''',"
                    + "  `flag_name` VARCHAR(255) NOT NULL COMMENT 'Lowercase flag name',"
                    + "  `params`    TEXT         COMMENT 'Space-separated flag parameters',"
                    + "  `value`     TINYINT(1)   NOT NULL DEFAULT 1 COMMENT '1 = active, 0 = explicitly unset',"
                    + "  PRIMARY KEY (`scope_id`, `flag_name`),"
                    + "  INDEX `idx_scope_id` (`scope_id`)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Load every stored flag row into a nested map.
     *
     * @return {@code Map<scopeId, Map<flagName, FlagEntry>>}
     * @throws SQLException on database error
     */
    public Map<String, Map<String, FlagEntry>> loadAllFlags() throws SQLException {
        Map<String, Map<String, FlagEntry>> result = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT `scope_id`, `flag_name`, `params`, `value` FROM `" + flagsTable + "`")) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String scopeId  = rs.getString("scope_id");
                String flagName = rs.getString("flag_name");
                String params   = rs.getString("params");
                boolean value   = rs.getBoolean("value");
                result.computeIfAbsent(scopeId, k -> new HashMap<>())
                      .put(flagName, new FlagEntry(params, value));
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Write – single row
    // -------------------------------------------------------------------------

    /**
     * Insert or update a single flag row (upsert).
     *
     * @param scopeId  scope identifier (claim ID, world name, etc.)
     * @param flagName lowercase flag name
     * @param params   serialised parameters (may be null / empty)
     * @param value    whether the flag is active
     * @throws SQLException on database error
     */
    public void saveFlag(@NotNull String scopeId,
                         @NotNull String flagName,
                         @Nullable String params,
                         boolean value) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO `" + flagsTable
                     + "` (`scope_id`, `flag_name`, `params`, `value`) VALUES (?, ?, ?, ?)"
                     + " ON DUPLICATE KEY UPDATE `params` = VALUES(`params`), `value` = VALUES(`value`)")) {
            stmt.setString(1, scopeId);
            stmt.setString(2, flagName);
            stmt.setString(3, params);
            stmt.setBoolean(4, value);
            stmt.executeUpdate();
        }
    }

    // -------------------------------------------------------------------------
    // Write – bulk
    // -------------------------------------------------------------------------

    /**
     * Replace the entire flags table with the supplied data inside a single transaction.
     * Intended for full-reload saves.
     *
     * @param allFlags nested map mirroring the in-memory flag store
     * @throws SQLException on database error
     */
    public void saveAllFlags(@NotNull Map<String, Map<String, FlagEntry>> allFlags) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement deleteAll = conn.createStatement();
                 PreparedStatement insert = conn.prepareStatement(
                         "INSERT INTO `" + flagsTable
                         + "` (`scope_id`, `flag_name`, `params`, `value`) VALUES (?, ?, ?, ?)")) {

                deleteAll.executeUpdate("DELETE FROM `" + flagsTable + "`");

                for (Map.Entry<String, Map<String, FlagEntry>> scopeEntry : allFlags.entrySet()) {
                    String scopeId = scopeEntry.getKey();
                    for (Map.Entry<String, FlagEntry> flagEntry : scopeEntry.getValue().entrySet()) {
                        FlagEntry fe = flagEntry.getValue();
                        insert.setString(1, scopeId);
                        insert.setString(2, flagEntry.getKey());
                        insert.setString(3, fe.params());
                        insert.setBoolean(4, fe.value());
                        insert.addBatch();
                    }
                }
                insert.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Delete
    // -------------------------------------------------------------------------

    /**
     * Delete a single flag row.
     *
     * @param scopeId  scope identifier
     * @param flagName lowercase flag name
     * @throws SQLException on database error
     */
    public void deleteFlag(@NotNull String scopeId, @NotNull String flagName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "DELETE FROM `" + flagsTable + "` WHERE `scope_id` = ? AND `flag_name` = ?")) {
            stmt.setString(1, scopeId);
            stmt.setString(2, flagName);
            stmt.executeUpdate();
        }
    }

    /**
     * Delete all flags that belong to any of the supplied scope IDs.
     * Used during stale-claim cleanup.
     *
     * @param scopeIds scope identifiers to purge
     * @throws SQLException on database error
     */
    public void deleteFlagsForScopes(@NotNull Collection<String> scopeIds) throws SQLException {
        if (scopeIds.isEmpty()) return;

        StringBuilder placeholders = new StringBuilder();
        boolean first = true;
        for (String ignored : scopeIds) {
            if (!first) placeholders.append(',');
            placeholders.append('?');
            first = false;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "DELETE FROM `" + flagsTable + "` WHERE `scope_id` IN (" + placeholders + ")")) {
            int idx = 1;
            for (String id : scopeIds) {
                stmt.setString(idx++, id);
            }
            stmt.executeUpdate();
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Closes the connection pool.  Safe to call even if never connected.
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            plugin.getLogger().info("MySQL connection pool closed.");
        }
    }

    // -------------------------------------------------------------------------
    // Value type
    // -------------------------------------------------------------------------

    /**
     * Lightweight holder for a single flag row read from the database.
     *
     * @param params serialised flag parameters (may be null)
     * @param value  whether the flag is active
     */
    public record FlagEntry(@Nullable String params, boolean value) {}
}
