using PostgresToMsSqlMigration.Models;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Utils;

public class DateValidator
{
    private readonly ILogger<DateValidator> _logger;

    public DateValidator(ILogger<DateValidator> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Validates date columns in a table's data and returns invalid date entries
    /// </summary>
    /// <param name="table">Table information</param>
    /// <param name="data">Data to validate</param>
    /// <returns>List of invalid date entries with details</returns>
    public List<InvalidDateEntry> ValidateDateColumns(TableInfo table, List<Dictionary<string, object>> data)
    {
        var invalidEntries = new List<InvalidDateEntry>();
        var dateColumns = GetDateColumns(table);

        if (!dateColumns.Any())
            return invalidEntries;

        for (int rowIndex = 0; rowIndex < data.Count; rowIndex++)
        {
            var row = data[rowIndex];
            
            foreach (var column in dateColumns)
            {
                if (row.ContainsKey(column.ColumnName))
                {
                    var value = row[column.ColumnName];
                    if (value != null && value != DBNull.Value)
                    {
                        if (!IsValidDate(value, column.DataType))
                        {
                            var idValue = GetIdValue(row, table);
                            invalidEntries.Add(new InvalidDateEntry
                            {
                                TableName = table.TableName,
                                ColumnName = column.ColumnName,
                                IdValue = idValue,
                                InvalidValue = value.ToString(),
                                DataType = column.DataType,
                                RowIndex = rowIndex
                            });
                        }
                    }
                }
            }
        }

        return invalidEntries;
    }

    /// <summary>
    /// Gets all date-related columns from a table
    /// </summary>
    /// <param name="table">Table information</param>
    /// <returns>List of date columns</returns>
    private List<ColumnInfo> GetDateColumns(TableInfo table)
    {
        return table.Columns.Where(c => IsDateColumn(c.DataType)).ToList();
    }

    /// <summary>
    /// Checks if a column data type is a date type
    /// </summary>
    /// <param name="dataType">SQL Server data type</param>
    /// <returns>True if it's a date type</returns>
    private bool IsDateColumn(string dataType)
    {
        var dateTypes = new[] { "date", "datetime", "datetime2", "datetimeoffset", "time", "timestamp", "timestampz" };
        return dateTypes.Contains(dataType.ToLower());
    }

    /// <summary>
    /// Validates if a value is a valid date for the given data type
    /// </summary>
    /// <param name="value">Value to validate</param>
    /// <param name="dataType">Expected data type</param>
    /// <returns>True if valid</returns>
    private bool IsValidDate(object value, string dataType)
    {
        try
        {
            var stringValue = value.ToString();
            
            // Handle empty strings
            if (string.IsNullOrWhiteSpace(stringValue))
                return false;

            return dataType.ToLower() switch
            {
                "date" => DateTime.TryParse(stringValue, out var date) && date >= DateTime.MinValue && date <= DateTime.MaxValue,
                "datetime" => DateTime.TryParse(stringValue, out var dateTime) && dateTime >= DateTime.MinValue && dateTime <= DateTime.MaxValue,
                "datetime2" => DateTime.TryParse(stringValue, out var dateTime2) && dateTime2 >= DateTime.MinValue && dateTime2 <= DateTime.MaxValue,
                "timestamp" => DateTime.TryParse(stringValue, out var dateTime2) && dateTime2 >= DateTime.MinValue && dateTime2 <= DateTime.MaxValue,
                "timestampz" => DateTime.TryParse(stringValue, out var dateTime2) && dateTime2 >= DateTime.MinValue && dateTime2 <= DateTime.MaxValue,
                "datetimeoffset" => DateTimeOffset.TryParse(stringValue, out _),
                "time" => TimeSpan.TryParse(stringValue, out _),
                _ => false
            };
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Gets the ID value from a row for identification purposes
    /// </summary>
    /// <param name="row">Data row</param>
    /// <param name="table">Table information</param>
    /// <returns>ID value or "Unknown" if not found</returns>
    private string GetIdValue(Dictionary<string, object> row, TableInfo table)
    {
        // Try to find an ID column (case-insensitive)
        var idColumn = table.Columns.FirstOrDefault(c => 
            c.ColumnName.Equals("id", StringComparison.OrdinalIgnoreCase) ||
            c.ColumnName.Equals("Id", StringComparison.OrdinalIgnoreCase) ||
            c.ColumnName.EndsWith("_id", StringComparison.OrdinalIgnoreCase) ||
            c.ColumnName.EndsWith("Id", StringComparison.OrdinalIgnoreCase));

        if (idColumn != null && row.ContainsKey(idColumn.ColumnName))
        {
            var value = row[idColumn.ColumnName];
            return value?.ToString() ?? "NULL";
        }

        // If no ID column found, try to use the first primary key
        if (table.PrimaryKeys.Any())
        {
            var pkColumn = table.PrimaryKeys.First();
            if (row.ContainsKey(pkColumn))
            {
                var value = row[pkColumn];
                return value?.ToString() ?? "NULL";
            }
        }

        return "Unknown";
    }

    /// <summary>
    /// Logs invalid date entries and throws an exception to quit the program
    /// </summary>
    /// <param name="invalidEntries">List of invalid date entries</param>
    public void LogInvalidDatesAndQuit(List<InvalidDateEntry> invalidEntries)
    {
        if (!invalidEntries.Any())
            return;

        _logger.LogError("INVALID DATE VALUES DETECTED! Migration will be stopped.");
        _logger.LogError("Total invalid date entries found: {Count}", invalidEntries.Count);

        Console.WriteLine();
        Console.WriteLine("=".PadRight(80, '='));
        Console.WriteLine("INVALID DATE VALUES DETECTED - MIGRATION STOPPED");
        Console.WriteLine("=".PadRight(80, '='));

        // Group by table for better readability
        var groupedEntries = invalidEntries.GroupBy(e => e.TableName);

        foreach (var group in groupedEntries)
        {
            Console.WriteLine($"\nTable: {group.Key}");
            Console.WriteLine("-".PadRight(40, '-'));

            foreach (var entry in group.Take(10)) // Show first 10 entries per table
            {
                Console.WriteLine($"  ID: {entry.IdValue}, Column: {entry.ColumnName}, Value: '{entry.InvalidValue}' (Expected: {entry.DataType})");
            }

            if (group.Count() > 10)
            {
                Console.WriteLine($"  ... and {group.Count() - 10} more invalid entries");
            }
        }

        Console.WriteLine("\n" + "=".PadRight(80, '='));
        Console.WriteLine("Please fix the invalid date values in PostgreSQL before running the migration again.");
        Console.WriteLine("=".PadRight(80, '='));
        Console.WriteLine();

        throw new InvalidOperationException($"Migration stopped due to {invalidEntries.Count} invalid date values found. Please check the console output for details.");
    }
}

public class InvalidDateEntry
{
    public string TableName { get; set; } = string.Empty;
    public string ColumnName { get; set; } = string.Empty;
    public string IdValue { get; set; } = string.Empty;
    public string InvalidValue { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public int RowIndex { get; set; }
} 