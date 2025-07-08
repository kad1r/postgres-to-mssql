namespace PostgresToMsSqlMigration.Utils;

public static class DefaultValueConverter
{
    /// <summary>
    /// Converts PostgreSQL default values to SQL Server equivalents
    /// </summary>
    /// <param name="postgreSqlDefault">The PostgreSQL default value</param>
    /// <returns>The SQL Server equivalent default value</returns>
    public static string ConvertToSqlServerDefault(string? postgreSqlDefault)
    {
        if (string.IsNullOrEmpty(postgreSqlDefault))
            return string.Empty;

        // Remove PostgreSQL type casting (e.g., ''::character varying -> '')
        var cleanedDefault = RemovePostgreSqlTypeCasting(postgreSqlDefault);

        // Handle invalid datetime defaults
        if (IsInvalidDateTimeDefault(cleanedDefault))
        {
            return "GETDATE()";
        }

        return cleanedDefault.ToLower() switch
        {
            // Date/Time functions
            "now()" => "GETDATE()",
            "current_timestamp" => "GETDATE()",
            "current_date" => "GETDATE()",
            "current_time" => "GETDATE()",
            
            // Boolean values
            "true" => "1",
            "false" => "0",
            
            // String literals (keep as is, but ensure proper quoting)
            var s when s.StartsWith("'") && s.EndsWith("'") => s,
            var s when s.StartsWith("\"") && s.EndsWith("\"") => $"'{s.Trim('"')}'",
            
            // Numeric values (keep as is)
            var n when IsNumeric(n) => n,
            
            // UUID generation
            "gen_random_uuid()" => "NEWID()",
            "uuid_generate_v4()" => "NEWID()",
            
            // Sequence functions (these should be handled as IDENTITY instead)
            var seq when seq.Contains("nextval") => string.Empty,
            
            // Default case - return as is
            _ => cleanedDefault
        };
    }

    /// <summary>
    /// Removes PostgreSQL type casting from default values
    /// </summary>
    /// <param name="defaultValue">The default value with potential type casting</param>
    /// <returns>The default value without type casting</returns>
    private static string RemovePostgreSqlTypeCasting(string defaultValue)
    {
        // Handle type casting patterns like 'value'::type or "value"::type
        var patterns = new[]
        {
            @"'([^']*)'::[a-zA-Z_][a-zA-Z0-9_]*",  // 'value'::type
            @"""([^""]*)""::[a-zA-Z_][a-zA-Z0-9_]*", // "value"::type
            @"([^'""\s]+)::[a-zA-Z_][a-zA-Z0-9_]*"   // value::type (no quotes)
        };

        foreach (var pattern in patterns)
        {
            var match = System.Text.RegularExpressions.Regex.Match(defaultValue, pattern);
            if (match.Success)
            {
                // Return the captured group (the actual value)
                return match.Groups[1].Value;
            }
        }

        return defaultValue;
    }

    /// <summary>
    /// Checks if a default value represents an invalid datetime for SQL Server
    /// </summary>
    /// <param name="defaultValue">The default value to check</param>
    /// <returns>True if the value is an invalid datetime default</returns>
    private static bool IsInvalidDateTimeDefault(string defaultValue)
    {
        // Common invalid datetime patterns that should be converted to GETDATE()
        var invalidDateTimePatterns = new[]
        {
            "0001-01-01 00:00:00",
            "1900-01-01 00:00:00",
            "1900-01-01",
            "0001-01-01",
            "1753-01-01", // SQL Server minimum date
            "1753-01-01 00:00:00"
        };

        return invalidDateTimePatterns.Any(pattern => 
            defaultValue.Equals(pattern, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Checks if a string represents a numeric value
    /// </summary>
    /// <param name="value">The string to check</param>
    /// <returns>True if the string is numeric</returns>
    private static bool IsNumeric(string value)
    {
        return double.TryParse(value, out _);
    }

    /// <summary>
    /// Determines if a default value should be converted to IDENTITY instead of DEFAULT
    /// </summary>
    /// <param name="postgreSqlDefault">The PostgreSQL default value</param>
    /// <returns>True if this should be an IDENTITY column</returns>
    public static bool ShouldBeIdentity(string? postgreSqlDefault)
    {
        if (string.IsNullOrEmpty(postgreSqlDefault))
            return false;

        return postgreSqlDefault.ToLower().Contains("nextval");
    }
} 