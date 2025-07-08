using Npgsql;
using PostgresToMsSqlMigration.Models;
using PostgresToMsSqlMigration.Utils;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Services;

public class PostgreSqlService
{
    private readonly string _connectionString;
    private readonly ILogger<PostgreSqlService> _logger;

    public PostgreSqlService(string connectionString, ILogger<PostgreSqlService> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task<List<TableInfo>> GetTablesAsync(List<string> skipTables)
    {
        var tables = new List<TableInfo>();
        
        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        // Get all tables excluding system tables and specified skip tables
        var tableQuery = @"
            SELECT 
                t.table_name,
                t.table_schema
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
                AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                AND t.table_name NOT IN ('" + string.Join("','", skipTables) + @"')
            ORDER BY t.table_schema, t.table_name";

        using var tableCommand = new NpgsqlCommand(tableQuery, connection);
        using var tableReader = await tableCommand.ExecuteReaderAsync();

        int tableNameOrdinal = tableReader.GetOrdinal("table_name");
        int schemaNameOrdinal = tableReader.GetOrdinal("table_schema");

        while (await tableReader.ReadAsync())
        {
            var tableName = tableReader.GetString(tableNameOrdinal);
            var schemaName = tableReader.GetString(schemaNameOrdinal);
            
            var tableInfo = new TableInfo
            {
                TableName = tableName,
                SchemaName = schemaName
            };

            tables.Add(tableInfo);
        }

        tableReader.Close();

        // Get columns, primary keys, foreign keys, and indexes for each table
        foreach (var table in tables)
        {
            await GetTableColumnsAsync(connection, table);
            await GetPrimaryKeysAsync(connection, table);
            await GetForeignKeysAsync(connection, table);
            await GetIndexesAsync(connection, table);
        }

        return tables;
    }

    private async Task GetTableColumnsAsync(NpgsqlConnection connection, TableInfo table)
    {
        var columnQuery = @"
            SELECT 
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                CASE WHEN c.column_default LIKE 'nextval%' THEN true ELSE false END as is_identity
            FROM information_schema.columns c
            WHERE c.table_name = @tableName 
                AND c.table_schema = @schemaName
            ORDER BY c.ordinal_position";

        using var command = new NpgsqlCommand(columnQuery, connection);
        command.Parameters.AddWithValue("@tableName", table.TableName);
        command.Parameters.AddWithValue("@schemaName", table.SchemaName);

        using var reader = await command.ExecuteReaderAsync();
        // Get ordinals for columns
        int columnNameOrdinal = reader.GetOrdinal("column_name");
        int dataTypeOrdinal = reader.GetOrdinal("data_type");
        int isNullableOrdinal = reader.GetOrdinal("is_nullable");
        int columnDefaultOrdinal = reader.GetOrdinal("column_default");
        int isIdentityOrdinal = reader.GetOrdinal("is_identity");
        int maxLengthOrdinal = reader.GetOrdinal("character_maximum_length");
        int precisionOrdinal = reader.GetOrdinal("numeric_precision");
        int scaleOrdinal = reader.GetOrdinal("numeric_scale");
        while (await reader.ReadAsync())
        {
            var column = new ColumnInfo
            {
                ColumnName = reader.GetString(columnNameOrdinal),
                DataType = MapPostgreSqlTypeToSqlServer(reader.GetString(dataTypeOrdinal), reader.GetString(columnNameOrdinal)),
                IsNullable = reader.GetString(isNullableOrdinal) == "YES",
                DefaultValue = reader.IsDBNull(columnDefaultOrdinal) ? null : reader.GetString(columnDefaultOrdinal),
                IsIdentity = reader.GetBoolean(isIdentityOrdinal),
                MaxLength = reader.IsDBNull(maxLengthOrdinal) ? null : reader.GetInt32(maxLengthOrdinal),
                Precision = reader.IsDBNull(precisionOrdinal) ? null : reader.GetInt32(precisionOrdinal),
                Scale = reader.IsDBNull(scaleOrdinal) ? null : reader.GetInt32(scaleOrdinal)
            };

            table.Columns.Add(column);
        }
    }

    private async Task GetPrimaryKeysAsync(NpgsqlConnection connection, TableInfo table)
    {
        var pkQuery = @"
            SELECT 
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_name = @tableName
                AND tc.table_schema = @schemaName
            ORDER BY kcu.ordinal_position";

        using var command = new NpgsqlCommand(pkQuery, connection);
        command.Parameters.AddWithValue("@tableName", table.TableName);
        command.Parameters.AddWithValue("@schemaName", table.SchemaName);

        using var reader = await command.ExecuteReaderAsync();
        int columnNameOrdinal = reader.GetOrdinal("column_name");
        while (await reader.ReadAsync())
        {
            table.PrimaryKeys.Add(reader.GetString(columnNameOrdinal));
        }
    }

    private async Task GetForeignKeysAsync(NpgsqlConnection connection, TableInfo table)
    {
        var fkQuery = @"
            SELECT 
                tc.constraint_name,
                kcu.column_name,
                ccu.table_name as referenced_table_name,
                ccu.column_name as referenced_column_name,
                ccu.table_schema as referenced_schema_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name = @tableName
                AND tc.table_schema = @schemaName";

        using var command = new NpgsqlCommand(fkQuery, connection);
        command.Parameters.AddWithValue("@tableName", table.TableName);
        command.Parameters.AddWithValue("@schemaName", table.SchemaName);

        using var reader = await command.ExecuteReaderAsync();
        int constraintNameOrdinal = reader.GetOrdinal("constraint_name");
        int columnNameOrdinal = reader.GetOrdinal("column_name");
        int referencedTableNameOrdinal = reader.GetOrdinal("referenced_table_name");
        int referencedColumnNameOrdinal = reader.GetOrdinal("referenced_column_name");
        int referencedSchemaNameOrdinal = reader.GetOrdinal("referenced_schema_name");
        while (await reader.ReadAsync())
        {
            var fk = new ForeignKeyInfo
            {
                ConstraintName = reader.GetString(constraintNameOrdinal),
                ColumnName = reader.GetString(columnNameOrdinal),
                ReferencedTableName = reader.GetString(referencedTableNameOrdinal),
                ReferencedColumnName = reader.GetString(referencedColumnNameOrdinal),
                ReferencedSchemaName = reader.GetString(referencedSchemaNameOrdinal)
            };

            table.ForeignKeys.Add(fk);
        }
    }

    private async Task GetIndexesAsync(NpgsqlConnection connection, TableInfo table)
    {
        var indexQuery = @"
            SELECT 
                i.relname as index_name,
                array_to_string(array_agg(a.attname), ',') as column_names,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary_key
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON ix.indexrelid = i.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relname = @tableName
                AND n.nspname = @schemaName
                AND t.relkind = 'r'
            GROUP BY i.relname, ix.indisunique, ix.indisprimary
            ORDER BY i.relname";

        using var command = new NpgsqlCommand(indexQuery, connection);
        command.Parameters.AddWithValue("@tableName", table.TableName);
        command.Parameters.AddWithValue("@schemaName", table.SchemaName);

        using var reader = await command.ExecuteReaderAsync();
        int indexNameOrdinal = reader.GetOrdinal("index_name");
        int columnNamesOrdinal = reader.GetOrdinal("column_names");
        int isUniqueOrdinal = reader.GetOrdinal("is_unique");
        int isPrimaryKeyOrdinal = reader.GetOrdinal("is_primary_key");
        while (await reader.ReadAsync())
        {
            var originalIndexName = reader.GetString(indexNameOrdinal);
            var cleanedIndexName = IndexNameCleaner.CleanIndexName(originalIndexName, table.TableName);
            
            // Log if the index name was cleaned
            if (originalIndexName != cleanedIndexName)
            {
                _logger.LogInformation("Cleaned index name for table {TableName}: '{OriginalName}' -> '{CleanedName}'", 
                    table.TableName, originalIndexName, cleanedIndexName);
            }
            
            var index = new IndexInfo
            {
                IndexName = cleanedIndexName,
                OriginalIndexName = originalIndexName, // Store original name for reporting
                ColumnNames = reader.GetString(columnNamesOrdinal).Split(',').ToList(),
                IsUnique = reader.GetBoolean(isUniqueOrdinal),
                IsPrimaryKey = reader.GetBoolean(isPrimaryKeyOrdinal)
            };

            table.Indexes.Add(index);
        }
    }

    public async Task<List<Dictionary<string, object>>> GetTableDataAsync(string tableName, string schemaName, int batchSize, int offset)
    {
        var data = new List<Dictionary<string, object>>();
        
        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var query = $"SELECT * FROM \"{schemaName}\".\"{tableName}\" LIMIT @batchSize OFFSET @offset";
        
        using var command = new NpgsqlCommand(query, connection);
        command.Parameters.AddWithValue("@batchSize", batchSize);
        command.Parameters.AddWithValue("@offset", offset);

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var value = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                row[columnName] = value;
            }
            data.Add(row);
        }

        return data;
    }

    public async Task<long> GetTableRowCountAsync(string tableName, string schemaName)
    {
        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var query = $"SELECT COUNT(*) FROM \"{schemaName}\".\"{tableName}\"";
        using var command = new NpgsqlCommand(query, connection);
        
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private string MapPostgreSqlTypeToSqlServer(string postgreSqlType, string? columnName = null)
    {
        // Special case: If column is named 'Id' and is a string type, map to uniqueidentifier
        if (!string.IsNullOrEmpty(columnName) && columnName.Equals("Id", StringComparison.OrdinalIgnoreCase))
        {
            var type = postgreSqlType.ToLower();
            if (type is "character varying" or "varchar" or "text" or "char" or "character")
                return "uniqueidentifier";
        }
        return postgreSqlType.ToLower() switch
        {
            // Integer types
            "integer" => "int",
            "int" => "int",
            "int4" => "int",
            "bigint" => "bigint",
            "int8" => "bigint",
            "smallint" => "smallint",
            "int2" => "smallint",
            
            // Serial types (auto-increment)
            "serial" => "int",
            "serial4" => "int",
            "bigserial" => "bigint",
            "serial8" => "bigint",
            "smallserial" => "smallint",
            "serial2" => "smallint",
            
            // Decimal/Numeric types
            "decimal" => "decimal",
            "numeric" => "decimal",
            "real" => "real",
            "float4" => "real",
            "double precision" => "float",
            "float8" => "float",
            "money" => "money",
            
            // Character types
            "character varying" => "nvarchar",
            "varchar" => "nvarchar",
            "character" => "nchar",
            "char" => "nchar",
            "text" => "nvarchar(max)",
            
            // Binary types
            "bytea" => "varbinary(max)",
            
            // Boolean type
            "boolean" => "bit",
            "bool" => "bit",
            
            // Date/Time types
            "date" => "date",
            "time" => "time",
            "time without time zone" => "time",
            "time with time zone" => "datetimeoffset",
            "timestamp" => "datetime2",
            "timestamp without time zone" => "datetime2",
            "timestamp with time zone" => "datetimeoffset",
            "timestamptz" => "datetimeoffset",
            "interval" => "nvarchar(50)",
            
            // UUID type
            "uuid" => "uniqueidentifier",
            
            // JSON types
            "json" => "text",
            "jsonb" => "text",
            
            // XML type
            "xml" => "xml",
            
            // Geometric types (convert to string)
            "point" => "nvarchar(50)",
            "line" => "nvarchar(100)",
            "lseg" => "nvarchar(100)",
            "box" => "nvarchar(100)",
            "path" => "nvarchar(max)",
            "polygon" => "nvarchar(max)",
            "circle" => "nvarchar(100)",
            
            // Network types (convert to string)
            "inet" => "nvarchar(45)",
            "cidr" => "nvarchar(45)",
            "macaddr" => "nvarchar(17)",
            "macaddr8" => "nvarchar(23)",
            
            // Bit string types
            "bit" => "varbinary",
            "bit varying" => "varbinary",
            
            // Default fallback
            _ => "nvarchar(max)"
        };
    }
} 