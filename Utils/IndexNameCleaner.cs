using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Utils;

public static class IndexNameCleaner
{
    /// <summary>
    /// Cleans an index name by replacing invalid characters with valid SQL Server identifier characters
    /// </summary>
    /// <param name="indexName">Original index name from PostgreSQL</param>
    /// <param name="tableName">Table name for generating fallback names</param>
    /// <returns>Cleaned index name suitable for SQL Server</returns>
    public static string CleanIndexName(string indexName, string tableName)
    {
        if (string.IsNullOrWhiteSpace(indexName))
            return GenerateDefaultIndexName(tableName);

        // Replace invalid characters with underscores
        var cleanedName = indexName
            .Replace("~", "_")
            .Replace("!", "_")
            .Replace("@", "_")
            .Replace("#", "_")
            .Replace("$", "_")
            .Replace("%", "_")
            .Replace("^", "_")
            .Replace("&", "_")
            .Replace("*", "_")
            .Replace("(", "_")
            .Replace(")", "_")
            .Replace("-", "_")
            .Replace("+", "_")
            .Replace("=", "_")
            .Replace("[", "_")
            .Replace("]", "_")
            .Replace("{", "_")
            .Replace("}", "_")
            .Replace("|", "_")
            .Replace("\\", "_")
            .Replace(":", "_")
            .Replace(";", "_")
            .Replace("\"", "_")
            .Replace("'", "_")
            .Replace("<", "_")
            .Replace(">", "_")
            .Replace(",", "_")
            .Replace(".", "_")
            .Replace("?", "_")
            .Replace("/", "_")
            .Replace(" ", "_");

        // Remove leading/trailing underscores
        cleanedName = cleanedName.Trim('_');

        // Ensure the name starts with a letter or underscore
        if (cleanedName.Length > 0 && !char.IsLetter(cleanedName[0]) && cleanedName[0] != '_')
        {
            cleanedName = "IX_" + cleanedName;
        }

        // Ensure the name is not empty
        if (string.IsNullOrWhiteSpace(cleanedName))
        {
            cleanedName = GenerateDefaultIndexName(tableName);
        }

        // Truncate if too long (SQL Server identifier limit is 128 characters)
        if (cleanedName.Length > 128)
        {
            cleanedName = cleanedName.Substring(0, 128);
        }

        return cleanedName;
    }

    /// <summary>
    /// Generates a default index name if the original name is invalid or empty
    /// </summary>
    /// <param name="tableName">Table name to use in the default name</param>
    /// <returns>Default index name</returns>
    private static string GenerateDefaultIndexName(string tableName)
    {
        var tableNameClean = tableName.Replace("~", "_").Trim('_');
        return $"IX_{tableNameClean}_{DateTime.UtcNow:yyyyMMddHHmmss}";
    }

    /// <summary>
    /// Checks if an index name needs cleaning
    /// </summary>
    /// <param name="indexName">Index name to check</param>
    /// <returns>True if the name contains invalid characters</returns>
    public static bool NeedsCleaning(string indexName)
    {
        if (string.IsNullOrWhiteSpace(indexName))
            return true;

        var invalidChars = new[] { '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', '=', '[', ']', '{', '}', '|', '\\', ':', ';', '"', '\'', '<', '>', ',', '.', '?', '/', ' ' };
        return invalidChars.Any(c => indexName.Contains(c));
    }
} 