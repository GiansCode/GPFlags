package me.ryanhamshire.GPFlags.commands;

import com.google.common.io.Files;
import me.ryanhamshire.GPFlags.DatabaseManager;
import me.ryanhamshire.GPFlags.FlagsDataStore;
import me.ryanhamshire.GPFlags.GPFlags;
import me.ryanhamshire.GPFlags.Messages;
import me.ryanhamshire.GPFlags.TextMode;
import me.ryanhamshire.GPFlags.util.MessagingUtil;
import me.ryanhamshire.GriefPrevention.GriefPrevention;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.command.PluginCommandYamlParser;
import org.bukkit.command.TabExecutor;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.PluginDescriptionFile;
import org.bukkit.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static org.bukkit.Bukkit.getName;

public class CommandGPFlags implements TabExecutor {
    @Override
    public boolean onCommand(@NotNull CommandSender commandSender, @NotNull Command command, @NotNull String s, @NotNull String[] args) {
        if (args.length > 0 && args[0].equalsIgnoreCase("reload")) {
            if (!commandSender.hasPermission("gpflags.command.reload")) {
                MessagingUtil.sendMessage(commandSender, TextMode.Err, Messages.NoCommandPermission, command.toString());
                return true;
            }
            me.ryanhamshire.GPFlags.GPFlags.getInstance().reloadConfig();
            GPFlags.getInstance().getFlagsDataStore().loadMessages();
            MessagingUtil.sendMessage(commandSender, TextMode.Success, Messages.ReloadComplete);
            return true;
        }

        if (args.length > 0 && args[0].equalsIgnoreCase("debug")) {
            if (!commandSender.hasPermission("gpflags.command.debug")) {
                MessagingUtil.sendMessage(commandSender, TextMode.Err, Messages.NoCommandPermission, command.toString());
                return true;
            }
            MessagingUtil.sendMessage(commandSender, "<gold>Server version: <yellow>" + Bukkit.getServer().getName() + " " + Bukkit.getServer().getVersion());
            MessagingUtil.sendMessage(commandSender, "<gold>GP version: <yellow>" + GriefPrevention.instance.getDescription().getVersion());
            MessagingUtil.sendMessage(commandSender, "<gold>GPF version: <yellow>" + GPFlags.getInstance().getDescription().getVersion());

            // Check other plugins hooking into GPFlags or GriefPrevention
            List<String> relatedPlugins = new ArrayList<>();
            for (Plugin plugin : Bukkit.getPluginManager().getPlugins()) {
                PluginDescriptionFile desc = plugin.getDescription();
                List<String> dependencies = new ArrayList<>();
                dependencies.addAll(desc.getDepend());
                dependencies.addAll(desc.getSoftDepend());
                if (dependencies.contains("GPFlags") || dependencies.contains("GriefPrevention") ) {
                    relatedPlugins.add(plugin.getName());
                }
            }
            relatedPlugins.remove("GPFlags");
            MessagingUtil.sendMessage(commandSender, "<gold>Dependent plugins: <yellow>" + String.join(" ", relatedPlugins));
            return true;
        }

        if (args.length > 0 && args[0].equalsIgnoreCase("importyaml")) {
            if (!commandSender.hasPermission("gpflags.command.importyaml")) {
                MessagingUtil.sendMessage(commandSender, TextMode.Err, Messages.NoCommandPermission, "importyaml");
                return true;
            }

            DatabaseManager db = GPFlags.getInstance().getDatabaseManager();
            if (db == null || !db.isConnected()) {
                MessagingUtil.sendMessage(commandSender, TextMode.Err +
                        "MySQL is not connected. Set <white>Database.Type: mysql</white> in config.yml and reload first.");
                return true;
            }

            File yamlFile = new File(FlagsDataStore.flagsFilePath);
            if (!yamlFile.exists()) {
                MessagingUtil.sendMessage(commandSender, TextMode.Err +
                        "flags.yml not found at <white>" + FlagsDataStore.flagsFilePath + "</white>. Nothing to migrate.");
                return true;
            }

            MessagingUtil.sendMessage(commandSender, TextMode.Info +
                    "Starting migration from <white>flags.yml</white> to MySQL — this may take a moment...");

            Bukkit.getScheduler().runTaskAsynchronously(GPFlags.getInstance(), () -> {
                try {
                    // Parse flags.yml into a map matching the DB schema
                    List<String> lines = Files.readLines(yamlFile, StandardCharsets.UTF_8);
                    StringBuilder builder = new StringBuilder();
                    for (String line : lines) builder.append(line).append('\n');

                    YamlConfiguration yaml = new YamlConfiguration();
                    yaml.loadFromString(builder.toString());

                    Map<String, Map<String, DatabaseManager.FlagEntry>> allFlags = new HashMap<>();
                    for (String scopeId : yaml.getKeys(false)) {
                        var scopeSection = yaml.getConfigurationSection(scopeId);
                        if (scopeSection == null) continue;
                        Map<String, DatabaseManager.FlagEntry> flagMap = new HashMap<>();
                        for (String flagName : scopeSection.getKeys(false)) {
                            String paramsDefault = yaml.getString(scopeId + "." + flagName);
                            String params = yaml.getString(scopeId + "." + flagName + ".params", paramsDefault);
                            boolean value  = yaml.getBoolean(scopeId + "." + flagName + ".value", true);
                            flagMap.put(flagName, new DatabaseManager.FlagEntry(params, value));
                        }
                        if (!flagMap.isEmpty()) allFlags.put(scopeId, flagMap);
                    }

                    int scopeCount = allFlags.size();
                    int flagCount  = allFlags.values().stream().mapToInt(Map::size).sum();

                    db.saveAllFlags(allFlags);

                    final String msg = TextMode.Success + "Migration complete: imported <white>" + flagCount
                            + "</white> flag(s) across <white>" + scopeCount + "</white> scope(s) into MySQL.";
                    Bukkit.getScheduler().runTask(GPFlags.getInstance(),
                            () -> MessagingUtil.sendMessage(commandSender, msg));

                } catch (SQLException e) {
                    GPFlags.getInstance().getLogger().log(Level.SEVERE, "flags.yml → MySQL migration failed", e);
                    final String err = TextMode.Err + "Migration failed (SQL error): " + e.getMessage()
                            + " — check the server log for details.";
                    Bukkit.getScheduler().runTask(GPFlags.getInstance(),
                            () -> MessagingUtil.sendMessage(commandSender, err));
                } catch (Exception e) {
                    GPFlags.getInstance().getLogger().log(Level.SEVERE, "flags.yml → MySQL migration failed", e);
                    final String err = TextMode.Err + "Migration failed: " + e.getMessage()
                            + " — check the server log for details.";
                    Bukkit.getScheduler().runTask(GPFlags.getInstance(),
                            () -> MessagingUtil.sendMessage(commandSender, err));
                }
            });
            return true;
        }

        if (!commandSender.hasPermission("gpflags.command.help")) {
            MessagingUtil.sendMessage(commandSender, TextMode.Err, Messages.NoCommandPermission, command.toString());
            return true;
        }
        List<Command> cmdList = PluginCommandYamlParser.parse(GPFlags.getInstance());
        for (Command c : cmdList) {
            if (c.getPermission() == null || commandSender.hasPermission(c.getPermission())) {
                MessagingUtil.sendMessage(commandSender, TextMode.Info + c.getUsage());
            }
        }
        return true;
    }

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender commandSender, @NotNull Command command, @NotNull String s, @NotNull String[] args) {
        ArrayList<String> list = new ArrayList<>();

        if (args.length == 1) {
            if (commandSender.hasPermission("gpflags.command.reload")) {
                list.add("reload");
            }
            if (commandSender.hasPermission("gpflags.command.debug")) {
                list.add("debug");
            }
            if (commandSender.hasPermission("gpflags.command.importyaml")) {
                list.add("importyaml");
            }
            if (commandSender.hasPermission("gpflags.command.help")) {
                list.add("help");
            }
            return StringUtil.copyPartialMatches(args[0], list, new ArrayList<>());
        }
        return null;
    }
}
