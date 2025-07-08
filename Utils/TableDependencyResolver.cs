using PostgresToMsSqlMigration.Models;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Utils;

public static class TableDependencyResolver
{
    /// <summary>
    /// Sorts tables in dependency order to avoid foreign key constraint violations during migration
    /// </summary>
    /// <param name="tables">List of tables to sort</param>
    /// <param name="logger">Logger for dependency information</param>
    /// <returns>Tables sorted in dependency order (parent tables first)</returns>
    public static List<TableInfo> SortTablesByDependencies(List<TableInfo> tables, ILogger logger)
    {
        var sortedTables = new List<TableInfo>();
        var visited = new HashSet<string>();
        var visiting = new HashSet<string>();
        var tableDict = tables.ToDictionary(t => t.TableName, t => t);

        // Build dependency graph
        var dependencies = BuildDependencyGraph(tables);

        // Log dependency information
        LogDependencyInfo(dependencies, logger);

        // Sort tables using topological sort
        foreach (var table in tables)
        {
            if (!visited.Contains(table.TableName))
            {
                if (!TopologicalSort(table.TableName, tableDict, dependencies, visited, visiting, sortedTables, logger))
                {
                    // If there's a circular dependency, fall back to alphabetical order
                    logger.LogWarning("Circular dependency detected, falling back to alphabetical order for table: {TableName}", table.TableName);
                    return tables.OrderBy(t => t.TableName).ToList();
                }
            }
        }

        logger.LogInformation("Tables sorted by dependencies. Migration order:");
        for (int i = 0; i < sortedTables.Count; i++)
        {
            logger.LogInformation("  {Index}: {TableName}", i + 1, sortedTables[i].TableName);
        }

        return sortedTables;
    }

    /// <summary>
    /// Builds a dependency graph from foreign key relationships
    /// </summary>
    /// <param name="tables">List of tables</param>
    /// <returns>Dictionary mapping table names to their dependencies</returns>
    private static Dictionary<string, List<string>> BuildDependencyGraph(List<TableInfo> tables)
    {
        var dependencies = new Dictionary<string, List<string>>();

        foreach (var table in tables)
        {
            dependencies[table.TableName] = new List<string>();

            foreach (var fk in table.ForeignKeys)
            {
                // Add dependency: current table depends on referenced table
                var referencedTable = fk.ReferencedTableName;
                if (tables.Any(t => t.TableName == referencedTable))
                {
                    dependencies[table.TableName].Add(referencedTable);
                }
            }
        }

        return dependencies;
    }

    /// <summary>
    /// Performs topological sort using DFS to handle dependencies
    /// </summary>
    /// <param name="tableName">Current table being processed</param>
    /// <param name="tableDict">Dictionary of all tables</param>
    /// <param name="dependencies">Dependency graph</param>
    /// <param name="visited">Set of visited tables</param>
    /// <param name="visiting">Set of tables currently being visited (for cycle detection)</param>
    /// <param name="sortedTables">List to store sorted tables</param>
    /// <param name="logger">Logger</param>
    /// <returns>True if successful, false if circular dependency detected</returns>
    private static bool TopologicalSort(
        string tableName,
        Dictionary<string, TableInfo> tableDict,
        Dictionary<string, List<string>> dependencies,
        HashSet<string> visited,
        HashSet<string> visiting,
        List<TableInfo> sortedTables,
        ILogger logger)
    {
        if (visiting.Contains(tableName))
        {
            logger.LogError("Circular dependency detected involving table: {TableName}", tableName);
            return false; // Circular dependency
        }

        if (visited.Contains(tableName))
        {
            return true; // Already processed
        }

        visiting.Add(tableName);

        // Process dependencies first
        if (dependencies.ContainsKey(tableName))
        {
            foreach (var dependency in dependencies[tableName])
            {
                if (!TopologicalSort(dependency, tableDict, dependencies, visited, visiting, sortedTables, logger))
                {
                    return false;
                }
            }
        }

        visiting.Remove(tableName);
        visited.Add(tableName);

        if (tableDict.ContainsKey(tableName))
        {
            sortedTables.Add(tableDict[tableName]);
        }

        return true;
    }

    /// <summary>
    /// Logs dependency information for debugging
    /// </summary>
    /// <param name="dependencies">Dependency graph</param>
    /// <param name="logger">Logger</param>
    private static void LogDependencyInfo(Dictionary<string, List<string>> dependencies, ILogger logger)
    {
        logger.LogInformation("Table dependency analysis:");
        foreach (var kvp in dependencies.OrderBy(x => x.Key))
        {
            if (kvp.Value.Count > 0)
            {
                logger.LogInformation("  {TableName} depends on: {Dependencies}", 
                    kvp.Key, string.Join(", ", kvp.Value));
            }
            else
            {
                logger.LogInformation("  {TableName} has no dependencies", kvp.Key);
            }
        }
    }

    /// <summary>
    /// Validates that all foreign key references point to existing tables
    /// </summary>
    /// <param name="tables">List of tables to validate</param>
    /// <param name="logger">Logger</param>
    /// <returns>True if all foreign keys are valid</returns>
    public static bool ValidateForeignKeys(List<TableInfo> tables, ILogger logger)
    {
        var tableNames = tables.Select(t => t.TableName).ToHashSet();
        var invalidForeignKeys = new List<(string TableName, string FkName, string ReferencedTable)>();

        foreach (var table in tables)
        {
            foreach (var fk in table.ForeignKeys)
            {
                if (!tableNames.Contains(fk.ReferencedTableName))
                {
                    invalidForeignKeys.Add((table.TableName, fk.ConstraintName, fk.ReferencedTableName));
                }
            }
        }

        if (invalidForeignKeys.Count > 0)
        {
            logger.LogWarning("Found {Count} foreign keys referencing non-existent tables:", invalidForeignKeys.Count);
            foreach (var (tableName, fkName, referencedTable) in invalidForeignKeys)
            {
                logger.LogWarning("  Table: {TableName}, FK: {FkName} -> {ReferencedTable} (not found)", 
                    tableName, fkName, referencedTable);
            }
            return false;
        }

        logger.LogInformation("All foreign key references are valid");
        return true;
    }
} 