namespace PostgresToMsSqlMigration.Models;

public class TableInfo
{
    public string TableName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public List<ColumnInfo> Columns { get; set; } = new();
    public List<string> PrimaryKeys { get; set; } = new();
    public List<ForeignKeyInfo> ForeignKeys { get; set; } = new();
    public List<IndexInfo> Indexes { get; set; } = new();
}

public class ColumnInfo
{
    public string ColumnName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public string? DefaultValue { get; set; }
    public bool IsIdentity { get; set; }
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
}

public class ForeignKeyInfo
{
    public string ConstraintName { get; set; } = string.Empty;
    public string ColumnName { get; set; } = string.Empty;
    public string ReferencedTableName { get; set; } = string.Empty;
    public string ReferencedColumnName { get; set; } = string.Empty;
    public string ReferencedSchemaName { get; set; } = string.Empty;
}

public class IndexInfo
{
    public string IndexName { get; set; } = string.Empty;
    public string OriginalIndexName { get; set; } = string.Empty;
    public List<string> ColumnNames { get; set; } = new();
    public bool IsUnique { get; set; }
    public bool IsPrimaryKey { get; set; }
} 