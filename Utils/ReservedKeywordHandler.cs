namespace PostgresToMsSqlMigration.Utils;

public static class ReservedKeywordHandler
{
    /// <summary>
    /// SQL Server reserved keywords that need to be bracketed
    /// </summary>
    private static readonly HashSet<string> ReservedKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN", "BREAK", "BROWSE", "BULK", "BY",
        "CASCADE", "CASE", "CHECK", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT", "COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
        "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK", "DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP", "DUMP",
        "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL", "FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION",
        "GOTO", "GRANT", "GROUP", "HAVING", "HOLDLOCK", "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN",
        "KEY", "KILL",
        "LEFT", "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", "NULLIF",
        "OF", "OFF", "OFFSETS", "ON", "OPEN", "OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER",
        "PERCENT", "PIVOT", "PLAN", "PRECISION", "PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC",
        "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE", "RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE",
        "SAVE", "SCHEMA", "SECURITYAUDIT", "SELECT", "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME", "STATISTICS", "SYSTEM_USER",
        "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT", "TSEQUAL",
        "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE", "WITH", "WRITETEXT"
    };

    /// <summary>
    /// Checks if a string is a SQL Server reserved keyword
    /// </summary>
    /// <param name="identifier">The identifier to check</param>
    /// <returns>True if the identifier is a reserved keyword</returns>
    public static bool IsReservedKeyword(string identifier)
    {
        return ReservedKeywords.Contains(identifier);
    }

    /// <summary>
    /// Escapes an identifier if it's a reserved keyword by adding square brackets
    /// </summary>
    /// <param name="identifier">The identifier to escape</param>
    /// <returns>The escaped identifier</returns>
    public static string EscapeIdentifier(string identifier)
    {
        if (IsReservedKeyword(identifier))
        {
            return $"[{identifier}]";
        }
        return identifier;
    }

    /// <summary>
    /// Escapes a list of identifiers
    /// </summary>
    /// <param name="identifiers">The identifiers to escape</param>
    /// <returns>List of escaped identifiers</returns>
    public static List<string> EscapeIdentifiers(IEnumerable<string> identifiers)
    {
        return identifiers.Select(EscapeIdentifier).ToList();
    }
} 