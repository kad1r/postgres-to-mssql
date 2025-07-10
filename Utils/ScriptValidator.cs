using PostgresToMsSqlMigration.Models;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Utils;

public static class ScriptValidator
{
    public static void ValidateScriptsForDuplicates(string scriptsDirectory, ILogger logger)
    {
        logger.LogInformation("Validating generated scripts for duplicate rows...");
        
        var scriptFiles = Directory.GetFiles(scriptsDirectory, "*.sql")
            .Where(f => !f.Contains("_create_") && !f.Contains("_bulk.sql")) // Exclude schema creation scripts
            .ToList();

        var tableGroups = scriptFiles
            .GroupBy(f => ExtractTableName(f))
            .ToList();

        foreach (var tableGroup in tableGroups)
        {
            var tableName = tableGroup.Key;
            var files = tableGroup.ToList();
            
            logger.LogInformation("Validating {FileCount} scripts for table: {TableName}", files.Count, tableName);
            
            var allRowKeys = new HashSet<string>();
            var duplicateCount = 0;
            
            foreach (var file in files.OrderBy(f => f))
            {
                var fileRowKeys = ExtractRowKeysFromScript(file, logger);
                
                foreach (var rowKey in fileRowKeys)
                {
                    if (!allRowKeys.Add(rowKey))
                    {
                        duplicateCount++;
                        logger.LogWarning("Duplicate row found in table {TableName}: {RowKey}", tableName, rowKey);
                    }
                }
            }
            
            if (duplicateCount > 0)
            {
                logger.LogWarning("Found {DuplicateCount} duplicate rows across {FileCount} scripts for table {TableName}",
                    duplicateCount, files.Count, tableName);
            }
            else
            {
                logger.LogInformation("No duplicate rows found in {FileCount} scripts for table {TableName}", files.Count, tableName);
            }
        }
    }

    private static string ExtractTableName(string filePath)
    {
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        
        // Remove batch information (e.g., "table_1-1000" -> "table")
        var underscoreIndex = fileName.LastIndexOf('_');
        if (underscoreIndex > 0)
        {
            var beforeUnderscore = fileName.Substring(0, underscoreIndex);
            
            // Check if the part after underscore is a range (e.g., "1-1000")
            var afterUnderscore = fileName.Substring(underscoreIndex + 1);
            if (afterUnderscore.Contains('-') && afterUnderscore.Split('-').Length == 2)
            {
                return beforeUnderscore;
            }
        }
        
        return fileName;
    }

    private static List<string> ExtractRowKeysFromScript(string filePath, ILogger logger)
    {
        var rowKeys = new List<string>();
        
        try
        {
            var lines = File.ReadAllLines(filePath);
            var inValuesSection = false;
            
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                
                // Skip comments and empty lines
                if (trimmedLine.StartsWith("--") || string.IsNullOrEmpty(trimmedLine))
                    continue;
                
                // Check if we're entering the VALUES section
                if (trimmedLine.StartsWith("INSERT INTO") && trimmedLine.Contains("VALUES"))
                {
                    inValuesSection = true;
                    continue;
                }
                
                // Check if we're exiting the VALUES section
                if (inValuesSection && (trimmedLine.StartsWith(";") || trimmedLine.StartsWith("COMMIT")))
                {
                    inValuesSection = false;
                    continue;
                }
                
                // Extract row values if we're in the VALUES section
                if (inValuesSection && trimmedLine.StartsWith("(") && trimmedLine.EndsWith(")"))
                {
                    var rowKey = ExtractRowKeyFromValuesLine(trimmedLine);
                    if (!string.IsNullOrEmpty(rowKey))
                    {
                        rowKeys.Add(rowKey);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading script file: {FilePath}", filePath);
        }
        
        return rowKeys;
    }

    private static string ExtractRowKeyFromValuesLine(string line)
    {
        try
        {
            // Remove outer parentheses
            var content = line.Trim('(', ')');
            
            // Split by comma, but be careful about commas within quoted strings
            var values = new List<string>();
            var currentValue = "";
            var inQuotes = false;
            var quoteChar = '\0';
            
            for (int i = 0; i < content.Length; i++)
            {
                var c = content[i];
                
                if (!inQuotes && (c == '\'' || c == 'N'))
                {
                    inQuotes = true;
                    quoteChar = c == 'N' ? '\'' : c;
                    currentValue += c;
                }
                else if (inQuotes && c == quoteChar)
                {
                    // Check for escaped quotes (double quotes)
                    if (i + 1 < content.Length && content[i + 1] == quoteChar)
                    {
                        currentValue += c;
                        i++; // Skip the next quote
                    }
                    else
                    {
                        inQuotes = false;
                        currentValue += c;
                    }
                }
                else if (!inQuotes && c == ',')
                {
                    values.Add(currentValue.Trim());
                    currentValue = "";
                }
                else
                {
                    currentValue += c;
                }
            }
            
            // Add the last value
            if (!string.IsNullOrEmpty(currentValue))
            {
                values.Add(currentValue.Trim());
            }
            
            // Create a key from the values (simplified - just concatenate)
            return string.Join("|", values);
        }
        catch
        {
            return "";
        }
    }
} 