namespace PostgresToMsSqlMigration.Models;

public enum MigrationMode
{
    SchemaOnly = 1,
    DataOnly = 2,
    Both = 3,
    DataScriptsOnly = 4
} 