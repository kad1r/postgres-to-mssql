using PostgresToMsSqlMigration.Models;
using PostgresToMsSqlMigration.Utils;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Services;

public class SqlScriptGenerator
{
    private readonly ILogger<SqlScriptGenerator> _logger;
    private readonly string _scriptsDirectory;
    private readonly bool _enableIdentityInsert;

    public SqlScriptGenerator(ILogger<SqlScriptGenerator> logger, bool enableIdentityInsert = true)
    {
        _logger = logger;
        _enableIdentityInsert = enableIdentityInsert;
        _scriptsDirectory = Path.Combine("files", "migration-scripts");

        // Ensure the scripts directory exists
        if (!Directory.Exists(_scriptsDirectory))
        {
            Directory.CreateDirectory(_scriptsDirectory);
        }
    }

    public async Task SaveDataMigrationScriptAsync
    (
        TableInfo table,
        List<Dictionary<string, object>> data,
        int batchNumber,
        int startRow,
        int endRow
    )
    {
        if (!data.Any()) return;

        // Remove duplicates from the data before generating script
        var uniqueData = RemoveDuplicateRows(data, table);
        
        if (uniqueData.Count != data.Count)
        {
            _logger.LogWarning("Removed {DuplicateCount} duplicate rows from batch {BatchNumber} for table {TableName}",
                data.Count - uniqueData.Count, batchNumber, table.TableName);
        }

        var tableName = CaseConverter.ToPascalCase(table.TableName);
        var escapedTableName = ReservedKeywordHandler.EscapeIdentifier(tableName);
        var columns = table.Columns.Select(c => ReservedKeywordHandler.EscapeIdentifier(CaseConverter.ToPascalCase(c.ColumnName))).ToList();
        var columnList = string.Join(", ", columns);

        // Check if table has identity columns
        var hasIdentityColumns = table.Columns.Any(c => c.IsIdentity);

        if (hasIdentityColumns)
        {
            if (_enableIdentityInsert)
            {
                _logger.LogInformation("Table {TableName} has identity columns - adding IDENTITY_INSERT statements to script", table.TableName);
            }
            else
            {
                _logger.LogWarning("Table {TableName} has identity columns but IDENTITY_INSERT is disabled in configuration - identity insert statements will not be added", table.TableName);
            }
        }

        var scriptContent = new List<string>
        {
            $"-- Data migration script for table: {table.TableName}",
            $"-- Batch: {batchNumber} (Rows {startRow}-{endRow})",
            $"-- Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC",
            $"-- Total rows in batch: {uniqueData.Count}",
            $"-- Performance optimized for bulk insert",
            $"-- Data ordered by primary key/ID to ensure consistency",
            hasIdentityColumns ? $"-- Table has identity columns - using IDENTITY_INSERT" : "",
            "",
            "SET NOCOUNT ON;",
            "BEGIN TRANSACTION;",
            ""
        };

        // Add identity insert on if table has identity columns
        if (hasIdentityColumns && _enableIdentityInsert)
        {
            scriptContent.Add($"SET IDENTITY_INSERT {escapedTableName} ON;");
            scriptContent.Add("");
        }

        scriptContent.Add($"INSERT INTO {escapedTableName} ({columnList}) VALUES");

        // Process data in chunks for better performance
        const int chunkSize = 1000;
        var allValueStatements = new List<string>();

        for (var i = 0; i < uniqueData.Count; i += chunkSize)
        {
            var chunk = uniqueData.Skip(i).Take(chunkSize);
            var chunkStatements = new List<string>();

            foreach (var row in chunk)
            {
                var values = new List<string>();

                foreach (var column in table.Columns)
                {
                    var columnName = column.ColumnName;
                    var value = row.ContainsKey(columnName) ? row[columnName] : DBNull.Value;

                    values.Add(FormatSqlValue(value, column.DataType));
                }

                chunkStatements.Add($"({string.Join(", ", values)})");
            }

            allValueStatements.Add(string.Join(",\n", chunkStatements));
        }

        // Add all chunks to the script
        for (var i = 0; i < allValueStatements.Count; i++)
        {
            if (i > 0)
            {
                scriptContent.Add(";");
                scriptContent.Add("");
                scriptContent.Add($"INSERT INTO {escapedTableName} ({columnList}) VALUES");
            }

            scriptContent.Add(allValueStatements[i]);
        }

        scriptContent.Add(";");
        scriptContent.Add("");

        // Add identity insert off if table has identity columns
        if (hasIdentityColumns && _enableIdentityInsert)
        {
            scriptContent.Add($"SET IDENTITY_INSERT {escapedTableName} OFF;");
            scriptContent.Add("");
        }

        scriptContent.Add("COMMIT;");
        scriptContent.Add("SET NOCOUNT OFF;");
        scriptContent.Add("");

        var fileName = $"{table.TableName}_{startRow}-{endRow}.sql";
        var filePath = Path.Combine(_scriptsDirectory, fileName);

        await File.WriteAllLinesAsync(filePath, scriptContent);

        _logger.LogDebug("Saved optimized data migration script: {FileName} ({RowCount} rows)", fileName, uniqueData.Count);
    }

    public async Task SaveHighPerformanceScriptAsync
    (
        TableInfo table,
        List<Dictionary<string, object>> data,
        int batchNumber,
        int startRow,
        int endRow
    )
    {
        if (data.Count == 0) return;

        // Remove duplicates from the data before generating script
        var uniqueData = RemoveDuplicateRows(data, table);
        
        if (uniqueData.Count != data.Count)
        {
            _logger.LogWarning("Removed {DuplicateCount} duplicate rows from high-performance batch {BatchNumber} for table {TableName}",
                data.Count - uniqueData.Count, batchNumber, table.TableName);
        }

        var tableName = CaseConverter.ToPascalCase(table.TableName);
        var escapedTableName = ReservedKeywordHandler.EscapeIdentifier(tableName);
        var columns = table.Columns.Select(c => ReservedKeywordHandler.EscapeIdentifier(CaseConverter.ToPascalCase(c.ColumnName))).ToList();
        var columnList = string.Join(", ", columns);

        // Check if table has identity columns
        var hasIdentityColumns = table.Columns.Any(c => c.IsIdentity);

        if (hasIdentityColumns)
        {
            _logger.LogInformation("Table {TableName} has identity columns - adding IDENTITY_INSERT statements to script", table.TableName);
        }

        var scriptContent = new List<string>
        {
            $"-- High-performance data migration script for table: {table.TableName}",
            $"-- Batch: {batchNumber} (Rows {startRow}-{endRow})",
            $"-- Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC",
            $"-- Total rows in batch: {uniqueData.Count}",
            $"-- Performance optimized with SQL Server hints",
            $"-- Data ordered by primary key/ID to ensure consistency",
            hasIdentityColumns ? $"-- Table has identity columns - using IDENTITY_INSERT" : "",
            "",
            "SET NOCOUNT ON;",
            "SET ARITHABORT ON;",
            "SET NUMERIC_ROUNDABORT OFF;",
            "SET CONCAT_NULL_YIELDS_NULL ON;",
            "SET ANSI_WARNINGS ON;",
            "SET ANSI_PADDING ON;",
            "SET ANSI_NULLS ON;",
            "SET QUOTED_IDENTIFIER ON;",
            "SET XACT_ABORT ON;",
            "",
            "BEGIN TRANSACTION;",
            ""
        };

        // Add identity insert on if table has identity columns
        if (hasIdentityColumns && _enableIdentityInsert)
        {
            scriptContent.Add($"SET IDENTITY_INSERT {escapedTableName} ON;");
            scriptContent.Add("");
        }

        scriptContent.Add($"INSERT INTO {escapedTableName} WITH (TABLOCK) ({columnList}) VALUES");

        // Process data in larger chunks for maximum performance
        const int chunkSize = 2000;
        var allValueStatements = new List<string>();

        for (var i = 0; i < uniqueData.Count; i += chunkSize)
        {
            var chunk = uniqueData.Skip(i).Take(chunkSize);
            var chunkStatements = new List<string>();

            foreach (var row in chunk)
            {
                var values = new List<string>();

                foreach (var column in table.Columns)
                {
                    var columnName = column.ColumnName;
                    var value = row.ContainsKey(columnName) ? row[columnName] : DBNull.Value;
                    values.Add(FormatSqlValue(value, column.DataType));
                }

                chunkStatements.Add($"({string.Join(", ", values)})");
            }

            allValueStatements.Add(string.Join(",\n", chunkStatements));
        }

        // Add all chunks to the script
        for (var i = 0; i < allValueStatements.Count; i++)
        {
            if (i > 0)
            {
                scriptContent.Add(";");
                scriptContent.Add("");
                scriptContent.Add($"INSERT INTO {escapedTableName} WITH (TABLOCK) ({columnList}) VALUES");
            }

            scriptContent.Add(allValueStatements[i]);
        }

        scriptContent.Add(";");
        scriptContent.Add("");

        // Add identity insert off if table has identity columns
        if (hasIdentityColumns && _enableIdentityInsert)
        {
            scriptContent.Add($"SET IDENTITY_INSERT {escapedTableName} OFF;");
            scriptContent.Add("");
        }

        scriptContent.Add("COMMIT;");
        scriptContent.Add("");
        scriptContent.Add("-- Reset session settings");
        scriptContent.Add("SET NOCOUNT OFF;");
        scriptContent.Add("SET ARITHABORT OFF;");
        scriptContent.Add("SET XACT_ABORT OFF;");
        scriptContent.Add("");

        var fileName = $"{table.TableName}_{startRow}-{endRow}_high_perf.sql";
        var filePath = Path.Combine(_scriptsDirectory, fileName);

        await File.WriteAllLinesAsync(filePath, scriptContent);

        _logger.LogDebug("Saved high-performance data migration script: {FileName} ({RowCount} rows)", fileName, uniqueData.Count);
    }

    public async Task SaveBulkInsertScriptAsync
    (
        TableInfo table,
        List<Dictionary<string, object>> data,
        int batchNumber,
        int startRow,
        int endRow
    )
    {
        if (data.Count == 0) return;

        // Remove duplicates from the data before generating script
        var uniqueData = RemoveDuplicateRows(data, table);
        
        if (uniqueData.Count != data.Count)
        {
            _logger.LogWarning("Removed {DuplicateCount} duplicate rows from bulk insert batch {BatchNumber} for table {TableName}",
                data.Count - uniqueData.Count, batchNumber, table.TableName);
        }

        var tableName = CaseConverter.ToPascalCase(table.TableName);
        var escapedTableName = ReservedKeywordHandler.EscapeIdentifier(tableName);
        var columns = table.Columns.Select(c => ReservedKeywordHandler.EscapeIdentifier(CaseConverter.ToPascalCase(c.ColumnName))).ToList();
        var columnList = string.Join(", ", columns);

        // Check if table has identity columns
        var hasIdentityColumns = table.Columns.Any(c => c.IsIdentity);

        if (hasIdentityColumns)
        {
            _logger.LogInformation("Table {TableName} has identity columns - adding IDENTITY_INSERT statements to script", table.TableName);
        }

        // Create CSV data file for bulk insert
        var csvFileName = $"{table.TableName}_{startRow}-{endRow}.csv";
        var csvFilePath = Path.Combine(_scriptsDirectory, csvFileName);

        // Write CSV data
        await using var csvWriter = new StreamWriter(csvFilePath);

        foreach (var row in uniqueData)
        {
            var csvValues = new List<string>();

            foreach (var column in table.Columns)
            {
                var columnName = column.ColumnName;
                var value = row.ContainsKey(columnName) ? row[columnName] : DBNull.Value;
                csvValues.Add(FormatCsvValue(value, column.DataType));
            }

            await csvWriter.WriteLineAsync(string.Join(",", csvValues));
        }

        // Create bulk insert script
        var scriptContent = new List<string>
        {
            $"-- Bulk insert script for table: {table.TableName}",
            $"-- Batch: {batchNumber} (Rows {startRow}-{endRow})",
            $"-- Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC",
            $"-- Total rows in batch: {uniqueData.Count}",
            $"-- CSV file: {csvFileName}",
            $"-- Performance optimized using BULK INSERT",
            $"-- Data ordered by primary key/ID to ensure consistency",
            hasIdentityColumns ? $"-- Table has identity columns - using IDENTITY_INSERT" : "",
            "",
            "SET NOCOUNT ON;",
            "BEGIN TRANSACTION;",
            ""
        };

        // Add identity insert on if table has identity columns
        if (hasIdentityColumns && _enableIdentityInsert)
        {
            scriptContent.Add($"SET IDENTITY_INSERT {escapedTableName} ON;");
            scriptContent.Add("");
        }

        scriptContent.Add($"BULK INSERT {escapedTableName}");
        scriptContent.Add($"FROM '{csvFilePath.Replace("\\", "\\\\")}'");
        scriptContent.Add("WITH (");
        scriptContent.Add("    FIRSTROW = 1,");
        scriptContent.Add("    FIELDTERMINATOR = ',',");
        scriptContent.Add("    ROWTERMINATOR = '\\n',");
        scriptContent.Add("    CODEPAGE = '65001',");
        scriptContent.Add("    DATAFILETYPE = 'char',");
        scriptContent.Add("    MAXERRORS = 0");
        scriptContent.Add(");");
        scriptContent.Add("");

        // Add identity insert off if table has identity columns
        if (hasIdentityColumns && _enableIdentityInsert)
        {
            scriptContent.Add($"SET IDENTITY_INSERT {escapedTableName} OFF;");
            scriptContent.Add("");
        }

        scriptContent.Add("COMMIT;");
        scriptContent.Add("SET NOCOUNT OFF;");
        scriptContent.Add("");

        var scriptFileName = $"{table.TableName}_{startRow}-{endRow}_bulk.sql";
        var scriptFilePath = Path.Combine(_scriptsDirectory, scriptFileName);

        await File.WriteAllLinesAsync(scriptFilePath, scriptContent);

        _logger.LogDebug("Saved bulk insert script: {FileName} ({RowCount} rows) with CSV: {CsvFile}",
            scriptFileName, uniqueData.Count, csvFileName);
    }

    private List<Dictionary<string, object>> RemoveDuplicateRows(List<Dictionary<string, object>> data, TableInfo table)
    {
        var uniqueData = new List<Dictionary<string, object>>();
        var seenRows = new HashSet<string>();

        foreach (var row in data)
        {
            var rowKey = CreateRowKey(row, table);
            if (!seenRows.Contains(rowKey))
            {
                seenRows.Add(rowKey);
                uniqueData.Add(row);
            }
        }

        return uniqueData;
    }

    private string CreateRowKey(Dictionary<string, object> row, TableInfo table)
    {
        // Create a unique key based on primary key columns if available, otherwise use all columns
        var keyColumns = table.PrimaryKeys.Any() ? table.PrimaryKeys : table.Columns.Select(c => c.ColumnName).ToList();
        
        var keyValues = new List<string>();
        foreach (var columnName in keyColumns)
        {
            var value = row.ContainsKey(columnName) ? row[columnName] : DBNull.Value;
            keyValues.Add(value?.ToString() ?? "NULL");
        }
        
        return string.Join("|", keyValues);
    }

    private static string FormatCsvValue(object value, string dataType)
    {
        if (value == DBNull.Value)
            return "";

        var stringValue = value.ToString() ?? "";

        // Escape quotes and wrap in quotes if contains comma, quote, or newline
        if (!stringValue.Contains(",") && !stringValue.Contains("\"") && !stringValue.Contains("\n") && !stringValue.Contains("\r")) return stringValue;

        stringValue = stringValue.Replace("\"", "\"\"");

        return $"\"{stringValue}\"";
    }

    private string FormatSqlValue(object value, string dataType)
    {
        if (value == DBNull.Value)
            return "NULL";

        return dataType.ToLower() switch
        {
            "uniqueidentifier" => FormatGuidValue(value),
            "bit" => FormatBooleanValue(value),
            "datetime2" or "datetime" or "date" => FormatDateTimeValue(value),
            "time" => FormatTimeValue(value),
            "decimal" or "numeric" or "money" => FormatNumericValue(value),
            "int" or "bigint" or "smallint" => FormatIntegerValue(value),
            "float" or "real" => FormatFloatValue(value),
            "text" => FormatTextValue(value), // Handle text type specifically
            _ => FormatStringValue(value) // Default to string formatting
        };
    }

    private static string FormatGuidValue(object value)
    {
        return Guid.TryParse(value.ToString(), out var guid) ? $"'{guid}'" : "NULL";
    }

    private static string FormatBooleanValue(object value)
    {
        if (bool.TryParse(value.ToString(), out var boolValue))
        {
            return boolValue ? "1" : "0";
        }

        return "0";
    }

    private static string FormatDateTimeValue(object value)
    {
        return DateTime.TryParse(value.ToString(), out var dateTime) ? $"'{dateTime:yyyy-MM-dd HH:mm:ss.fff}'" : "NULL";
    }

    private static string FormatTimeValue(object value)
    {
        return TimeSpan.TryParse(value.ToString(), out var timeSpan) ? $"'{timeSpan:hh\\:mm\\:ss}'" : "NULL";
    }

    private static string FormatNumericValue(object value)
    {
        return decimal.TryParse(value.ToString(), out var decimalValue) ? decimalValue.ToString(System.Globalization.CultureInfo.InvariantCulture) : "0";
    }

    private static string FormatIntegerValue(object value)
    {
        return long.TryParse(value.ToString(), out var longValue) ? longValue.ToString() : "0";
    }

    private static string FormatFloatValue(object value)
    {
        return double.TryParse(value.ToString(), out var doubleValue) ? doubleValue.ToString(System.Globalization.CultureInfo.InvariantCulture) : "0.0";
    }

    private static string FormatStringValue(object value)
    {
        var stringValue = value.ToString() ?? "";
        // Escape single quotes by doubling them
        stringValue = stringValue.Replace("'", "''");

        return $"N'{stringValue}'";
    }

    private static string FormatTextValue(object value)
    {
        var stringValue = value.ToString() ?? "";
        // Escape single quotes by doubling them
        stringValue = stringValue.Replace("'", "''");

        // For text type, use single quotes without N prefix
        return $"'{stringValue}'";
    }

    public async Task SaveTableCreationScriptAsync(TableInfo table, string createTableSql)
    {
        var fileName = $"{table.TableName}_create_table.sql";
        var filePath = Path.Combine(_scriptsDirectory, fileName);

        var scriptContent = new List<string>
        {
            $"-- Table creation script for: {table.TableName}",
            $"-- Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC",
            $"-- Schema: {table.SchemaName}",
            "",
            createTableSql,
            ""
        };

        await File.WriteAllLinesAsync(filePath, scriptContent);

        _logger.LogDebug("Saved table creation script: {FileName}", fileName);
    }

    public async Task SaveIndexCreationScriptAsync(TableInfo table, List<string> indexScripts)
    {
        if (indexScripts.Count == 0) return;

        var fileName = $"{table.TableName}_create_indexes.sql";
        var filePath = Path.Combine(_scriptsDirectory, fileName);

        var scriptContent = new List<string>
        {
            $"-- Index creation script for: {table.TableName}",
            $"-- Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC",
            $"-- Schema: {table.SchemaName}",
            $"-- Total indexes: {indexScripts.Count}",
            ""
        };

        scriptContent.AddRange(indexScripts);
        scriptContent.Add("");

        await File.WriteAllLinesAsync(filePath, scriptContent);

        _logger.LogDebug("Saved index creation script: {FileName} ({IndexCount} indexes)", fileName, indexScripts.Count);
    }

    public async Task SaveForeignKeyCreationScriptAsync(TableInfo table, List<string> foreignKeyScripts)
    {
        if (foreignKeyScripts.Count == 0) return;

        var fileName = $"{table.TableName}_create_foreign_keys.sql";
        var filePath = Path.Combine(_scriptsDirectory, fileName);

        var scriptContent = new List<string>
        {
            $"-- Foreign key creation script for: {table.TableName}",
            $"-- Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC",
            $"-- Schema: {table.SchemaName}",
            $"-- Total foreign keys: {foreignKeyScripts.Count}",
            ""
        };

        scriptContent.AddRange(foreignKeyScripts);
        scriptContent.Add("");

        await File.WriteAllLinesAsync(filePath, scriptContent);

        _logger.LogDebug("Saved foreign key creation script: {FileName} ({FkCount} foreign keys)", fileName, foreignKeyScripts.Count);
    }
}