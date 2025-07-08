namespace PostgresToMsSqlMigration.Models;

public class ColumnRenameInfo
{
    public string TableName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public List<ColumnRename> RenamedColumns { get; set; } = new();
}

public class ColumnRename
{
    public string OriginalName { get; set; } = string.Empty;
    public string PascalCaseName { get; set; } = string.Empty;
    public string FinalName { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
}

public class IndexRenameInfo
{
    public string TableName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public List<IndexRename> RenamedIndexes { get; set; } = new();
}

public class IndexRename
{
    public string OriginalName { get; set; } = string.Empty;
    public string CleanedName { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
}

public class MigrationReport
{
    public DateTime MigrationDate { get; set; } = DateTime.Now;
    public List<ColumnRenameInfo> Tables { get; set; } = new();
    public List<IndexRenameInfo> IndexRenames { get; set; } = new();
    public int TotalRenamedColumns { get; set; }
    public int TotalRenamedIndexes { get; set; }
    public int TotalTables { get; set; }
} 