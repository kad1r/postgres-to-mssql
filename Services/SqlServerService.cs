using Microsoft.Data.SqlClient;
using PostgresToMsSqlMigration.Models;
using PostgresToMsSqlMigration.Utils;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Services;

public class SqlServerService(string connectionString, ILogger<SqlServerService> logger)
{
    public async Task CreateTableAsync(TableInfo table)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var createTableSql = GenerateCreateTableSql(table);
        logger.LogInformation("Creating table: {TableName}", CaseConverter.ToPascalCase(table.TableName));

        await using var command = new SqlCommand(createTableSql, connection);

        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);

            throw;
        }
    }

    public async Task CreateIndexesAsync(TableInfo table)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        foreach (var index in table.Indexes.Where(i => !i.IsPrimaryKey))
        {
            var createIndexSql = GenerateCreateIndexSql(table, index);
            logger.LogInformation("Creating index: {IndexName} on table: {TableName}",
                CaseConverter.ToPascalCase(index.IndexName), CaseConverter.ToPascalCase(table.TableName));

            await using var command = new SqlCommand(createIndexSql, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task CreateForeignKeysAsync(TableInfo table)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        foreach (var fk in table.ForeignKeys)
        {
            var createFkSql = GenerateCreateForeignKeySql(table, fk);
            logger.LogInformation("Creating foreign key: {ConstraintName} on table: {TableName}",
                CaseConverter.ToPascalCase(fk.ConstraintName), CaseConverter.ToPascalCase(table.TableName));

            await using var command = new SqlCommand(createFkSql, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task InsertDataAsync(TableInfo table, List<Dictionary<string, object>> data)
    {
        if (data.Count == 0) return;

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var tableName = CaseConverter.ToPascalCase(table.TableName);
        var escapedTableName = ReservedKeywordHandler.EscapeIdentifier(tableName);
        var columns = table.Columns.Select(c => ReservedKeywordHandler.EscapeIdentifier(CaseConverter.ToPascalCase(c.ColumnName))).ToList();
        var columnList = string.Join(", ", columns);
        var parameterList = string.Join(", ", columns.Select(c => "@" + c.Trim('[', ']')));

        var insertSql = $"INSERT INTO {escapedTableName} ({columnList}) VALUES ({parameterList})";

        await using var command = new SqlCommand(insertSql, connection);

        foreach (var row in data)
        {
            command.Parameters.Clear();

            foreach (var kvp in row)
            {
                var paramName = "@" + CaseConverter.ToPascalCase(kvp.Key);
                var value = kvp.Value == DBNull.Value ? DBNull.Value : kvp.Value;
                command.Parameters.AddWithValue(paramName, value);
            }

            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task<bool> TableExistsAsync(string tableName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        const string query = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName";

        await using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@tableName", CaseConverter.ToPascalCase(tableName));

        var count = Convert.ToInt32(await command.ExecuteScalarAsync());

        return count > 0;
    }

    public async Task DropTableIfExistsAsync(string tableName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var dropSql = $"IF OBJECT_ID('{CaseConverter.ToPascalCase(tableName)}', 'U') IS NOT NULL DROP TABLE {CaseConverter.ToPascalCase(tableName)}";

        await using var command = new SqlCommand(dropSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public static string GenerateCreateTableSql(TableInfo table)
    {
        var tableName = CaseConverter.ToPascalCase(table.TableName);
        var escapedTableName = ReservedKeywordHandler.EscapeIdentifier(tableName);
        var columns = new List<string>();

        foreach (var column in table.Columns)
        {
            var columnName = CaseConverter.ToPascalCase(column.ColumnName);
            var escapedColumnName = ReservedKeywordHandler.EscapeIdentifier(columnName);
            var dataType = GetSqlServerDataType(column);
            var nullable = column.IsNullable ? "NULL" : "NOT NULL";

            // Handle identity columns (sequences in PostgreSQL)
            var identity = "";
            var defaultValue = "";

            if (column.IsIdentity || DefaultValueConverter.ShouldBeIdentity(column.DefaultValue))
            {
                identity = "IDENTITY(1,1)";
            }
            else if (!string.IsNullOrEmpty(column.DefaultValue))
            {
                var sqlServerDefault = DefaultValueConverter.ConvertToSqlServerDefault(column.DefaultValue);

                if (!string.IsNullOrEmpty(sqlServerDefault))
                {
                    defaultValue = $"DEFAULT {sqlServerDefault}";
                }
            }

            var columnDef = $"{escapedColumnName} {dataType} {nullable} {identity} {defaultValue}".Trim();
            columns.Add(columnDef);
        }

        // Add primary key constraint
        if (table.PrimaryKeys.Count != 0)
        {
            var pkColumns = string.Join(", ", table.PrimaryKeys.Select(pk => ReservedKeywordHandler.EscapeIdentifier(CaseConverter.ToPascalCase(pk))));
            columns.Add($"CONSTRAINT PK_{tableName} PRIMARY KEY ({pkColumns})");
        }

        return $"CREATE TABLE {escapedTableName} (\n  {string.Join(",\n  ", columns)}\n)";
    }

    private static string GenerateCreateIndexSql(TableInfo table, IndexInfo index)
    {
        var tableName = CaseConverter.ToPascalCase(table.TableName);
        var escapedTableName = ReservedKeywordHandler.EscapeIdentifier(tableName);
        var indexName = CaseConverter.ToPascalCase(index.IndexName);
        var escapedIndexName = ReservedKeywordHandler.EscapeIdentifier(indexName);
        var columnNames = string.Join(", ", index.ColumnNames.Select(c => ReservedKeywordHandler.EscapeIdentifier(CaseConverter.ToPascalCase(c))));
        var unique = index.IsUnique ? "UNIQUE" : "";

        return $"CREATE {unique} INDEX {escapedIndexName} ON {escapedTableName} ({columnNames})";
    }

    private static string GenerateCreateForeignKeySql(TableInfo table, ForeignKeyInfo fk)
    {
        var tableName = CaseConverter.ToPascalCase(table.TableName);
        var escapedTableName = ReservedKeywordHandler.EscapeIdentifier(tableName);
        var constraintName = CaseConverter.ToPascalCase(fk.ConstraintName);
        var escapedConstraintName = ReservedKeywordHandler.EscapeIdentifier(constraintName);
        var columnName = CaseConverter.ToPascalCase(fk.ColumnName);
        var escapedColumnName = ReservedKeywordHandler.EscapeIdentifier(columnName);
        var referencedTableName = CaseConverter.ToPascalCase(fk.ReferencedTableName);
        var escapedReferencedTableName = ReservedKeywordHandler.EscapeIdentifier(referencedTableName);
        var referencedColumnName = CaseConverter.ToPascalCase(fk.ReferencedColumnName);
        var escapedReferencedColumnName = ReservedKeywordHandler.EscapeIdentifier(referencedColumnName);

        return $"ALTER TABLE {escapedTableName} ADD CONSTRAINT {escapedConstraintName.Replace("~", "")} FOREIGN KEY ({escapedColumnName}) REFERENCES {escapedReferencedTableName} ({escapedReferencedColumnName})";
    }

    private static string GetSqlServerDataType(ColumnInfo column)
    {
        var baseType = column.DataType;

        return baseType switch
        {
            "nvarchar" when column.MaxLength.HasValue => $"nvarchar({column.MaxLength})",
            "varchar" when column.MaxLength.HasValue => $"varchar({column.MaxLength})",
            "decimal" when column is { Precision: not null, Scale: not null } => $"decimal({column.Precision},{column.Scale})",
            "nvarchar" when !column.MaxLength.HasValue => "nvarchar(max)",
            "varchar" when !column.MaxLength.HasValue => "varchar(max)",
            _ => baseType
        };
    }

    public static List<string> GenerateIndexCreationScripts(TableInfo table)
    {
        var scripts = new List<string>();

        foreach (var index in table.Indexes.Where(i => !i.IsPrimaryKey))
        {
            var script = GenerateCreateIndexSql(table, index);
            scripts.Add(script);
        }

        return scripts;
    }

    public static List<string> GenerateForeignKeyCreationScripts(TableInfo table)
    {
        var scripts = new List<string>();

        foreach (var fk in table.ForeignKeys)
        {
            var script = GenerateCreateForeignKeySql(table, fk);
            scripts.Add(script);
        }

        return scripts;
    }

    public async Task DisableForeignKeyConstraintsAsync()
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        const string disableFkSql = @"
            EXEC sp_MSforeachtable @command1='ALTER TABLE ? NOCHECK CONSTRAINT ALL'";

        await using var command = new SqlCommand(disableFkSql, connection);
        await command.ExecuteNonQueryAsync();

        logger.LogInformation("Disabled foreign key constraints for all tables");
    }

    public async Task EnableForeignKeyConstraintsAsync()
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        const string enableFkSql = @"
            EXEC sp_MSforeachtable @command1='ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'";

        await using var command = new SqlCommand(enableFkSql, connection);
        await command.ExecuteNonQueryAsync();

        logger.LogInformation("Re-enabled foreign key constraints for all tables");
    }
}