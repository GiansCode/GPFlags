package me.ryanhamshire.GPFlags;

import com.google.common.io.Files;
import me.ryanhamshire.GPFlags.flags.FlagDefinition;
import me.ryanhamshire.GPFlags.util.MessagingUtil;
import me.ryanhamshire.GriefPrevention.Claim;
import me.ryanhamshire.GriefPrevention.GriefPrevention;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.OfflinePlayer;
import org.bukkit.World;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Manager for flags.
 * Inherited = Checks higher levels
 * Self = Doesn't check higher levels
 * Raw = Will return the flag for flags that are set to be unset
 * Logical = Will return null for flags that are set to be unset
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class FlagManager {

    private final ConcurrentHashMap<String, FlagDefinition> definitions;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Flag>> flags;
    private final List<String> worlds = new ArrayList<>();

    /** Flag used during bulk-load to suppress per-row DB writes. */
    private boolean isLoading = false;
    /** Set after {@link #setDatabaseManager(DatabaseManager)} is called. */
    private DatabaseManager databaseManager = null;

    public static final String DEFAULT_FLAG_ID = "-2";

    public FlagManager() {
        this.definitions = new ConcurrentHashMap<>();
        this.flags = new ConcurrentHashMap<>();
        Bukkit.getWorlds().forEach(world -> worlds.add(world.getName()));
    }

    // -------------------------------------------------------------------------
    // Database integration
    // -------------------------------------------------------------------------

    /**
     * Attach (or replace) the {@link DatabaseManager}.
     * Pass {@code null} to revert to YAML-only storage.
     */
    public void setDatabaseManager(@Nullable DatabaseManager db) {
        this.databaseManager = db;
    }

    /**
     * Asynchronously persist a single flag row to MySQL.
     * No-op if no DB is configured or the plugin is currently loading.
     */
    private void asyncSaveFlag(String scopeId, String flagName, String params, boolean value) {
        if (isLoading || databaseManager == null || !databaseManager.isConnected()) return;
        Bukkit.getScheduler().runTaskAsynchronously(GPFlags.getInstance(), () -> {
            try {
                databaseManager.saveFlag(scopeId, flagName, params, value);
            } catch (SQLException e) {
                GPFlags.getInstance().getLogger().log(Level.WARNING,
                        "Failed to save flag '" + flagName + "' for scope '" + scopeId + "' to MySQL.", e);
            }
        });
    }

    /**
     * Asynchronously delete a single flag row from MySQL.
     * No-op if no DB is configured or the plugin is currently loading.
     */
    private void asyncDeleteFlag(String scopeId, String flagName) {
        if (isLoading || databaseManager == null || !databaseManager.isConnected()) return;
        Bukkit.getScheduler().runTaskAsynchronously(GPFlags.getInstance(), () -> {
            try {
                databaseManager.deleteFlag(scopeId, flagName);
            } catch (SQLException e) {
                GPFlags.getInstance().getLogger().log(Level.WARNING,
                        "Failed to delete flag '" + flagName + "' for scope '" + scopeId + "' from MySQL.", e);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Flag definition registry
    // -------------------------------------------------------------------------

    /**
     * Register a new flag definition.
     *
     * @param def Flag Definition to register
     */
    public void registerFlagDefinition(FlagDefinition def) {
        this.definitions.put(def.getName().toLowerCase(), def);
    }

    /**
     * Get a flag definition by name.
     *
     * @param name Name of the flag to get
     * @return Flag definition by name
     */
    public FlagDefinition getFlagDefinitionByName(String name) {
        return this.definitions.get(name.toLowerCase());
    }

    /**
     * Get a collection of all registered flag definitions.
     *
     * @return All registered flag definitions
     */
    public Collection<FlagDefinition> getFlagDefinitions() {
        return new ArrayList<>(this.definitions.values());
    }

    /**
     * Get a collection of names of all registered flag definitions.
     *
     * @return Names of all registered flag definitions
     */
    public Collection<String> getFlagDefinitionNames() {
        return new ArrayList<>(this.definitions.keySet());
    }

    // -------------------------------------------------------------------------
    // Flag mutation
    // -------------------------------------------------------------------------

    /**
     * Set a flag for a claim.  Called on startup to populate the datastore and
     * whenever a flag is set (including to {@code false}).
     *
     * @param claimId  ID of the {@link Claim} this flag belongs to
     * @param def      Flag definition
     * @param isActive Whether the flag will be active
     * @param sender   Command sender (may be null during load)
     * @param args     Flag parameters
     * @return Result of the operation
     */
    public SetFlagResult setFlag(String claimId, FlagDefinition def, boolean isActive, CommandSender sender, String... args) {
        StringBuilder internalParameters = new StringBuilder();
        StringBuilder friendlyParameters = new StringBuilder();
        for (String arg : args) {
            friendlyParameters.append(arg).append(" ");
            if (def.getName().equals("NoEnterPlayer") && !arg.isEmpty()) {
                if (arg.length() <= 30) {
                    OfflinePlayer offlinePlayer;
                    try {
                        offlinePlayer = Bukkit.getOfflinePlayerIfCached(arg);
                        if (offlinePlayer != null) {
                            arg = offlinePlayer.getUniqueId().toString();
                        }
                    } catch (NoSuchMethodError ignored) {
                        offlinePlayer = Bukkit.getOfflinePlayer(arg);
                        arg = offlinePlayer.getUniqueId().toString();
                    }
                }
            }
            internalParameters.append(arg).append(" ");
        }
        internalParameters = new StringBuilder(internalParameters.toString().trim());
        friendlyParameters = new StringBuilder(friendlyParameters.toString().trim());

        SetFlagResult result;
        if (isActive) {
            result = def.validateParameters(friendlyParameters.toString(), sender);
            if (!result.success) return result;
        } else {
            result = new SetFlagResult(true, def.getUnSetMessage());
        }

        Flag flag = new Flag(def, internalParameters.toString());
        flag.setSet(isActive);
        ConcurrentHashMap<String, Flag> claimFlags = this.flags.get(claimId);
        if (claimFlags == null) {
            claimFlags = new ConcurrentHashMap<>();
            this.flags.put(claimId, claimFlags);
        }

        String key = def.getName().toLowerCase();
        if (!claimFlags.containsKey(key) && isActive) {
            def.incrementInstances();
        }
        claimFlags.put(key, flag);

        // Persist to MySQL (write-through, async)
        asyncSaveFlag(claimId, key, flag.parameters, isActive);

        // Notify definitions for default flags
        if (DEFAULT_FLAG_ID.equals(claimId)) {
            for (Claim claim : GriefPrevention.instance.dataStore.getClaims()) {
                if (isActive) {
                    def.onFlagSet(claim, flag.parameters);
                } else {
                    def.onFlagUnset(claim);
                }
            }
            return result;
        }

        Claim claim;
        try {
            claim = GriefPrevention.instance.dataStore.getClaim(Long.parseLong(claimId));
        } catch (Throwable ignored) {
            return result;
        }
        if (claim != null) {
            if (isActive) {
                def.onFlagSet(claim, internalParameters.toString());
            } else {
                def.onFlagUnset(claim);
            }
        }
        return result;
    }

    /**
     * @param claim claim or subclaim
     * @param flag  flag name in all lowercase
     * @return The raw instance of the flag
     */
    public @Nullable Flag getRawClaimFlag(@NotNull Claim claim, @NotNull String flag) {
        ConcurrentHashMap<String, Flag> claimFlags = this.flags.get(claim.getID().toString());
        if (claimFlags == null) return null;
        return claimFlags.get(flag);
    }

    public @Nullable Flag getRawDefaultFlag(@NotNull String flag) {
        ConcurrentHashMap<String, Flag> defaultFlags = flags.get(DEFAULT_FLAG_ID);
        if (defaultFlags == null) return null;
        return defaultFlags.get(flag);
    }

    public @Nullable Flag getRawWorldFlag(@NotNull World world, @NotNull String flag) {
        ConcurrentHashMap<String, Flag> worldFlags = flags.get(world.getName());
        if (worldFlags == null) return null;
        return worldFlags.get(flag);
    }

    public @Nullable Flag getRawServerFlag(@NotNull String flag) {
        ConcurrentHashMap<String, Flag> serverFlags = this.flags.get("everywhere");
        if (serverFlags == null) return null;
        return serverFlags.get(flag);
    }

    /**
     * @param location location to look up
     * @param flagname flag name
     * @param claim    optional pre-resolved claim
     * @return the effective flag at the location
     */
    public @Nullable Flag getEffectiveFlag(@Nullable Location location, @NotNull String flagname, @Nullable Claim claim) {
        if (location == null) return null;
        flagname = flagname.toLowerCase();
        Flag flag;
        if (GriefPrevention.instance.claimsEnabledForWorld(location.getWorld())) {
            if (claim != null) {
                flag = getRawClaimFlag(claim, flagname);
                if (flag != null) {
                    if (flag.getSet()) return flag;
                    return null;
                }
                Claim parent = claim.parent;
                if (parent != null) {
                    flag = getRawClaimFlag(parent, flagname);
                    if (flag != null) {
                        if (flag.getSet()) return flag;
                        return null;
                    }
                }
                flag = getRawDefaultFlag(flagname);
                if (flag != null) {
                    if (flag.getSet()) return flag;
                    return null;
                }
            }
        }

        flag = getRawWorldFlag(location.getWorld(), flagname);
        if (flag != null && flag.getSet()) return flag;

        flag = getRawServerFlag(flagname);
        if (flag != null && flag.getSet()) return flag;

        return null;
    }

    /**
     * @param flagname flag name
     * @param claim    optional pre-resolved claim
     * @param world    world to check if claim is null
     * @return the effective flag for the given context
     */
    public @Nullable Flag getEffectiveFlag(@NotNull String flagname, @Nullable Claim claim, @NotNull World world) {
        flagname = flagname.toLowerCase();
        Flag flag;
        if (claim != null && GriefPrevention.instance.claimsEnabledForWorld(world)) {
            flag = getRawClaimFlag(claim, flagname);
            if (flag != null) {
                if (flag.getSet()) return flag;
                return null;
            }
            Claim parent = claim.parent;
            if (parent != null) {
                flag = getRawClaimFlag(parent, flagname);
                if (flag != null) {
                    if (flag.getSet()) return flag;
                    return null;
                }
            }
            flag = getRawDefaultFlag(flagname);
            if (flag != null) {
                if (flag.getSet()) return flag;
                return null;
            }
        }

        flag = getRawWorldFlag(world, flagname);
        if (flag != null && flag.getSet()) return flag;

        flag = getRawServerFlag(flagname);
        if (flag != null && flag.getSet()) return flag;

        return null;
    }

    /**
     * Get all flags in a claim.
     *
     * @param claim Claim to get flags from
     * @return All flags in this claim
     */
    public Collection<Flag> getFlags(Claim claim) {
        if (claim == null) return null;
        return getFlags(claim.getID().toString());
    }

    /**
     * Get all flags in a claim.
     *
     * @param claimID ID of claim
     * @return All flags in this claim
     */
    public Collection<Flag> getFlags(String claimID) {
        if (claimID == null) return null;
        ConcurrentHashMap<String, Flag> claimFlags = this.flags.get(claimID);
        if (claimFlags == null) {
            return new ArrayList<>();
        } else {
            return new ArrayList<>(claimFlags.values());
        }
    }

    /**
     * Unset a flag in a claim.
     *
     * @param claim Claim to remove flag from
     * @param def   Flag definition to remove
     * @return Flag result
     */
    public SetFlagResult unSetFlag(Claim claim, FlagDefinition def) {
        return unSetFlag(claim.getID().toString(), def);
    }

    /**
     * Unset a flag in a claim.
     *
     * @param claimId ID of claim
     * @param def     Flag definition to remove
     * @return Flag result
     */
    public SetFlagResult unSetFlag(String claimId, FlagDefinition def) {
        ConcurrentHashMap<String, Flag> claimFlags = this.flags.get(claimId);
        if (claimFlags == null || !claimFlags.containsKey(def.getName().toLowerCase())) {
            return this.setFlag(claimId, def, false, null);
        } else {
            try {
                Claim claim = GriefPrevention.instance.dataStore.getClaim(Long.parseLong(claimId));
                def.onFlagUnset(claim);
            } catch (Throwable ignored) {}
            String flagKey = def.getName().toLowerCase();
            claimFlags.remove(flagKey);
            asyncDeleteFlag(claimId, flagKey);
            return new SetFlagResult(true, def.getUnSetMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Persistence – load
    // -------------------------------------------------------------------------

    /**
     * Load flags from a YAML-formatted string.
     * If a {@link DatabaseManager} is connected this method is still available
     * for migration purposes but normal startup uses {@link #loadFromDatabase()}.
     */
    List<MessageSpecifier> load(String input) throws InvalidConfigurationException {
        this.flags.clear();
        isLoading = true;
        YamlConfiguration yaml = new YamlConfiguration();
        yaml.loadFromString(input);

        ArrayList<MessageSpecifier> errors = new ArrayList<>();
        Set<String> claimIDs = yaml.getKeys(false);
        for (String claimID : claimIDs) {
            Set<String> flagNames = yaml.getConfigurationSection(claimID).getKeys(false);
            for (String flagName : flagNames) {
                String paramsDefault = yaml.getString(claimID + "." + flagName);
                String params = yaml.getString(claimID + "." + flagName + ".params", paramsDefault);
                if (FlagsDataStore.PRIOR_CONFIG_VERSION == 0) {
                    params = MessagingUtil.reserialize(params);
                }
                boolean set = yaml.getBoolean(claimID + "." + flagName + ".value", true);
                FlagDefinition def = this.getFlagDefinitionByName(flagName);
                if (def != null) {
                    SetFlagResult result = this.setFlag(claimID, def, set, null, params);
                    if (!result.success) {
                        errors.add(result.message);
                    }
                }
            }
        }
        isLoading = false;
        if (errors.isEmpty() && FlagsDataStore.PRIOR_CONFIG_VERSION == 0) save();
        return errors;
    }

    /**
     * Load all flags from MySQL.  Should only be called after flag definitions
     * have been registered.
     *
     * @return A list of validation errors (empty on full success)
     */
    public List<MessageSpecifier> loadFromDatabase() {
        this.flags.clear();
        isLoading = true;
        ArrayList<MessageSpecifier> errors = new ArrayList<>();
        try {
            Map<String, Map<String, DatabaseManager.FlagEntry>> allFlags =
                    databaseManager.loadAllFlags();
            for (Map.Entry<String, Map<String, DatabaseManager.FlagEntry>> scopeEntry : allFlags.entrySet()) {
                String claimID = scopeEntry.getKey();
                for (Map.Entry<String, DatabaseManager.FlagEntry> flagEntry : scopeEntry.getValue().entrySet()) {
                    String flagName = flagEntry.getKey();
                    DatabaseManager.FlagEntry fe = flagEntry.getValue();
                    FlagDefinition def = getFlagDefinitionByName(flagName);
                    if (def != null) {
                        String params = fe.params() != null ? fe.params() : "";
                        SetFlagResult result = setFlag(claimID, def, fe.value(), null, params);
                        if (!result.success) {
                            errors.add(result.message);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            GPFlags.getInstance().getLogger().log(Level.SEVERE,
                    "Failed to load flags from MySQL.", e);
        } finally {
            isLoading = false;
        }
        return errors;
    }

    // -------------------------------------------------------------------------
    // Persistence – save
    // -------------------------------------------------------------------------

    /**
     * Persist all in-memory flags.
     * Uses MySQL when a connected {@link DatabaseManager} is available,
     * otherwise falls back to YAML.
     */
    public void save() {
        if (databaseManager != null && databaseManager.isConnected()) {
            saveToDatabase();
        } else {
            try {
                this.save(FlagsDataStore.flagsFilePath);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    /** Async bulk-save of the entire in-memory flag store to MySQL. */
    private void saveToDatabase() {
        Map<String, Map<String, DatabaseManager.FlagEntry>> snapshot = buildDatabaseSnapshot();
        Bukkit.getScheduler().runTaskAsynchronously(GPFlags.getInstance(), () -> {
            try {
                databaseManager.saveAllFlags(snapshot);
            } catch (SQLException e) {
                GPFlags.getInstance().getLogger().log(Level.WARNING,
                        "Failed to bulk-save flags to MySQL.", e);
            }
        });
    }

    /** Build a serialisable snapshot of the current in-memory flag state. */
    private Map<String, Map<String, DatabaseManager.FlagEntry>> buildDatabaseSnapshot() {
        Map<String, Map<String, DatabaseManager.FlagEntry>> snapshot = new HashMap<>();
        for (Map.Entry<String, ConcurrentHashMap<String, Flag>> scopeEntry : this.flags.entrySet()) {
            Map<String, DatabaseManager.FlagEntry> flagMap = new HashMap<>();
            for (Map.Entry<String, Flag> flagEntry : scopeEntry.getValue().entrySet()) {
                Flag f = flagEntry.getValue();
                flagMap.put(flagEntry.getKey(), new DatabaseManager.FlagEntry(f.parameters, f.getSet()));
            }
            snapshot.put(scopeEntry.getKey(), flagMap);
        }
        return snapshot;
    }

    public HashSet<String> getUsedFlags() {
        HashSet<String> usedFlags = new HashSet<>();
        for (ConcurrentHashMap<String, Flag> claimFlags : this.flags.values()) {
            usedFlags.addAll(claimFlags.keySet());
        }
        return usedFlags;
    }

    public String flagsToString() {
        YamlConfiguration yaml = new YamlConfiguration();
        for (Map.Entry<String, ConcurrentHashMap<String, Flag>> scopeEntry : this.flags.entrySet()) {
            String claimID = scopeEntry.getKey();
            for (Map.Entry<String, Flag> flagEntry : scopeEntry.getValue().entrySet()) {
                Flag flag = flagEntry.getValue();
                yaml.set(claimID + "." + flagEntry.getKey() + ".params", flag.parameters);
                yaml.set(claimID + "." + flagEntry.getKey() + ".value", flag.getSet());
            }
        }
        return yaml.saveToString();
    }

    public void save(String filepath) throws IOException {
        String fileContent = this.flagsToString();
        File file = new File(filepath);
        file.getParentFile().mkdirs();
        file.createNewFile();
        Files.write(fileContent.getBytes(StandardCharsets.UTF_8), file);
    }

    /**
     * Load flags from a file (YAML format).
     *
     * @param file Source file
     * @return A list of errors
     */
    public List<MessageSpecifier> load(File file) throws IOException, InvalidConfigurationException {
        if (!file.exists()) return this.load("");

        List<String> lines = Files.readLines(file, StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append('\n');
        }

        return this.load(builder.toString());
    }

    public void clear() {
        this.flags.clear();
    }

    /**
     * Remove flags whose scope IDs are no longer valid claim IDs,
     * then persist the cleanup both in-memory and in the backing store.
     */
    void removeExceptClaimIDs(HashSet<String> validClaimIDs) {
        HashSet<String> toRemove = new HashSet<>();
        for (String key : this.flags.keySet()) {
            if (!validClaimIDs.contains(key)) {
                try {
                    int numericalValue = Integer.parseInt(key);
                    // Negative special values (like DEFAULT_FLAG_ID = "-2") are kept
                    if (numericalValue >= 0) toRemove.add(key);
                } catch (NumberFormatException ignore) {
                    // Non-numeric keys are world/server scopes – keep them
                }
            }
        }
        for (String key : toRemove) {
            this.flags.remove(key);
        }

        // Purge stale rows from MySQL asynchronously
        if (databaseManager != null && databaseManager.isConnected() && !toRemove.isEmpty()) {
            final HashSet<String> finalToRemove = new HashSet<>(toRemove);
            Bukkit.getScheduler().runTaskAsynchronously(GPFlags.getInstance(), () -> {
                try {
                    databaseManager.deleteFlagsForScopes(finalToRemove);
                } catch (SQLException e) {
                    GPFlags.getInstance().getLogger().log(Level.WARNING,
                            "Failed to purge stale claim flags from MySQL.", e);
                }
            });
        }

        save();
    }
}
