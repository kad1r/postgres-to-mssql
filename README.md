# PostgreSQL to SQL Server Migration Tool

A .NET 8 console application that migrates schema and data from PostgreSQL to Microsoft SQL Server. The tool automatically handles case conversion from snake_case (PostgreSQL) to PascalCase (SQL Server) and provides comprehensive logging throughout the migration process.

## Features

- **Schema Migration**: Automatically extracts and recreates table schemas
- **Data Migration**: Migrates all data in configurable batches
- **Case Conversion**: Converts snake_case (PostgreSQL) to PascalCase (SQL Server)
- **Index & Foreign Key Support**: Recreates indexes and foreign key constraints
- **Comprehensive Logging**: Detailed logging of all migration steps
- **Configurable**: Easy configuration via appsettings.json
- **Skip Tables**: Ability to skip specific tables (e.g., __EFMigrations)
- **Batch Processing**: Configurable batch size for large datasets

## Prerequisites

- .NET 8.0 SDK or Runtime
- PostgreSQL database (source)
- Microsoft SQL Server database (target)
- Network access to both databases

## Installation

1. Clone or download this repository
2. Navigate to the project directory
3. Restore NuGet packages:
   ```bash
   dotnet restore
   ```

## Configuration

Edit the `appsettings.json` file to configure your database connections and migration settings:

```json
{
  "ConnectionStrings": {
    "PostgreSQL": "Host=localhost;Database=your_postgres_db;Username=your_username;Password=your_password",
    "SqlServer": "Server=localhost;Database=your_mssql_db;Trusted_Connection=true;TrustServerCertificate=true"
  },
  "MigrationSettings": {
    "BatchSize": 1000,
    "SkipTables": ["__EFMigrations"],
    "SkipStoredProcedures": true,
    "SkipViews": true,
    "EnableLogging": true
  }
}
```

### Connection String Examples

**PostgreSQL:**
```
Host=localhost;Database=mydb;Username=postgres;Password=mypassword;Port=5432
```

**SQL Server:**
```
Server=localhost;Database=mydb;Trusted_Connection=true;TrustServerCertificate=true
```
or with SQL authentication:
```
Server=localhost;Database=mydb;User Id=sa;Password=mypassword;TrustServerCertificate=true
```

## Usage

1. **Configure your connection strings** in `appsettings.json`
2. **Run the migration**:
   ```bash
   dotnet run
   ```

The application will:
1. Extract schema information from PostgreSQL
2. Create tables in SQL Server
3. Migrate data in batches
4. Create indexes and foreign key constraints
5. Provide detailed logging throughout the process

## Migration Process

The migration follows these steps:

1. **Schema Extraction**: Reads table definitions, columns, primary keys, foreign keys, and indexes from PostgreSQL
2. **Table Creation**: Creates tables in SQL Server with proper data type mapping
3. **Data Migration**: Migrates data in configurable batches to handle large datasets efficiently
4. **Constraint Creation**: Creates indexes and foreign key constraints after data migration

## Data Type Mapping

The tool automatically maps PostgreSQL data types to SQL Server equivalents:

| PostgreSQL | SQL Server |
|------------|------------|
| integer | int |
| bigint | bigint |
| smallint | smallint |
| serial | int (IDENTITY) |
| decimal/numeric | decimal |
| real | real |
| double precision | float |
| character varying/varchar | nvarchar |
| text | nvarchar(max) |
| boolean | bit |
| date | date |
| timestamp | datetime2 |
| timestamp with time zone | datetimeoffset |
| uuid | uniqueidentifier |
| json/jsonb | nvarchar(max) |

## Configuration Options

- **BatchSize**: Number of rows to process in each batch (default: 1000)
- **SkipTables**: Array of table names to skip during migration
- **SkipStoredProcedures**: Skip stored procedure migration (currently not implemented)
- **SkipViews**: Skip view migration (currently not implemented)

## Logging

The application provides comprehensive logging including:
- Migration progress
- Table creation status
- Data migration progress
- Error details
- Performance metrics

## Error Handling

The application includes robust error handling:
- Connection validation
- Schema validation
- Data type compatibility checks
- Detailed error messages with context

## Limitations

- Stored procedures and views are not currently migrated
- Some advanced PostgreSQL features may not have direct SQL Server equivalents
- Large object (LOB) data types may require special handling
- Custom PostgreSQL extensions are not supported

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify connection strings and network connectivity
2. **Permission Errors**: Ensure database user has appropriate permissions
3. **Data Type Errors**: Check for unsupported data types in your schema
4. **Memory Issues**: Reduce batch size for very large tables

### Debug Mode

To enable more detailed logging, modify the logging level in `Program.cs`:

```csharp
builder.SetMinimumLevel(LogLevel.Debug);
```

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is provided as-is for educational and development purposes. 