using PostgresToMsSqlMigration.Models;
using PostgresToMsSqlMigration.Utils;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Services;

public class MigrationService
(
    PostgreSqlService postgreSqlService,
    SqlServerService sqlServerService,
    ReportService reportService,
    SqlScriptGenerator scriptGenerator,
    DateValidator dateValidator,
    ILogger<MigrationService> logger,
    int batchSize = 1000,
    string scriptGenerationMethod = "Optimized",
    bool useBulkInsert = false,
    bool disableForeignKeyConstraints = true)
{
    public async Task MigrateAsync(List<string> skipTables, List<string> skipDataMigrationTables, MigrationMode mode)
    {
        logger.LogInformation("Starting PostgreSQL to SQL Server migration...");
        logger.LogInformation("Migration mode: {Mode}", mode);

        var schemaCreationTime = TimeSpan.Zero;
        var dataMigrationTime = TimeSpan.Zero;
        var totalStartTime = DateTime.UtcNow;

        try
        {
            // Step 1: Get all tables from PostgreSQL
            logger.LogInformation("Extracting schema from PostgreSQL...");
            var tables = await postgreSqlService.GetTablesAsync(skipTables);
            logger.LogInformation("Found {TableCount} tables to migrate", tables.Count);

            // Validate foreign key references
            TableDependencyResolver.ValidateForeignKeys(tables, logger);

            // Sort tables by dependencies to avoid foreign key constraint violations
            logger.LogInformation("Analyzing table dependencies for proper migration order...");
            var sortedTables = TableDependencyResolver.SortTablesByDependencies(tables, logger);

            // Step 2: Create tables in SQL Server (Schema Creation)
            if (mode == MigrationMode.SchemaOnly || mode == MigrationMode.Both)
            {
                logger.LogInformation("Creating tables in SQL Server...");
                var schemaStartTime = DateTime.UtcNow;

                foreach (var table in sortedTables)
                {
                    await CreateTableInSqlServerAsync(table);
                    reportService.AddTableRenameInfo(table);
                }

                schemaCreationTime = DateTime.UtcNow - schemaStartTime;
                logger.LogInformation("Schema creation completed in {SchemaTime}", schemaCreationTime);

                // Step 4: Create indexes and foreign keys
                logger.LogInformation("Creating indexes and foreign keys...");

                foreach (var table in sortedTables)
                {
                    await CreateIndexesAndForeignKeysAsync(table);
                }
            }

            // Step 3: Migrate data
            if (mode == MigrationMode.DataOnly || mode == MigrationMode.Both)
            {
                logger.LogInformation("Migrating data...");
                logger.LogInformation("Script generation method: {Method}, Use bulk insert: {UseBulkInsert}",
                    scriptGenerationMethod, useBulkInsert);
                var dataStartTime = DateTime.UtcNow;

                // Filter tables for data migration and maintain dependency order
                var dataMigrationTables = sortedTables.Where(t => !skipDataMigrationTables.Contains(t.TableName)).ToList();
                logger.LogInformation("Migrating data for {TableCount} tables (skipping {SkipCount} tables)",
                    dataMigrationTables.Count, skipDataMigrationTables.Count);

                // Disable foreign key constraints during data migration
                await DisableForeignKeyConstraintsAsync();

                try
                {
                    foreach (var table in dataMigrationTables)
                    {
                        await MigrateTableDataAsync(table);
                    }
                }
                finally
                {
                    // Re-enable foreign key constraints after data migration
                    await EnableForeignKeyConstraintsAsync();
                }

                dataMigrationTime = DateTime.UtcNow - dataStartTime;
                logger.LogInformation("Data migration completed in {DataTime}", dataMigrationTime);
            }

            var totalTime = DateTime.UtcNow - totalStartTime;

            // Display timing summary
            DisplayTimingSummary(schemaCreationTime, dataMigrationTime, totalTime);

            logger.LogInformation("Migration completed successfully!");

            // Generate and save the migration report
            await reportService.SaveReportAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Migration failed: {ErrorMessage}", ex.Message);

            throw;
        }
    }

    private async Task CreateTableInSqlServerAsync(TableInfo table)
    {
        try
        {
            // Drop table if it exists
            await sqlServerService.DropTableIfExistsAsync(table.TableName);

            // Get the create table SQL for script generation
            var createTableSql = SqlServerService.GenerateCreateTableSql(table);

            // Create the table
            await sqlServerService.CreateTableAsync(table);
            logger.LogInformation("Created table: {TableName}", table.TableName);

            // Save the table creation script
            await scriptGenerator.SaveTableCreationScriptAsync(table, createTableSql);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create table {TableName}: {ErrorMessage}", table.TableName, ex.Message);

            throw;
        }
    }

    private async Task MigrateTableDataAsync(TableInfo table)
    {
        try
        {
            var totalRows = await postgreSqlService.GetTableRowCountAsync(table.TableName, table.SchemaName);
            logger.LogInformation("Migrating {RowCount} rows from table: {TableName}", totalRows, table.TableName);

            if (totalRows == 0)
            {
                logger.LogInformation("Table {TableName} is empty, skipping data migration", table.TableName);

                return;
            }

            var offset = 0;
            var migratedRows = 0;
            var batchNumber = 1;

            while (offset < totalRows)
            {
                var batch = await postgreSqlService.GetTableDataAsync(table.TableName, table.SchemaName, batchSize, offset);

                if (batch.Any())
                {
                    // Validate date columns before processing
                    var invalidDates = dateValidator.ValidateDateColumns(table, batch);

                    if (invalidDates.Any())
                    {
                        dateValidator.LogInvalidDatesAndQuit(invalidDates);
                    }

                    // Save the data migration script for this batch
                    var startRow = offset + 1;
                    var endRow = offset + batch.Count;

                    if (useBulkInsert)
                    {
                        await scriptGenerator.SaveBulkInsertScriptAsync(table, batch, batchNumber, startRow, endRow);
                    }
                    else if (scriptGenerationMethod.Equals("HighPerformance", StringComparison.OrdinalIgnoreCase))
                    {
                        await scriptGenerator.SaveHighPerformanceScriptAsync(table, batch, batchNumber, startRow, endRow);
                    }
                    else
                    {
                        await scriptGenerator.SaveDataMigrationScriptAsync(table, batch, batchNumber, startRow, endRow);
                    }

                    await sqlServerService.InsertDataAsync(table, batch);

                    migratedRows += batch.Count;
                    logger.LogInformation("Migrated {MigratedRows}/{TotalRows} rows from table: {TableName} (Batch {BatchNumber})",
                        migratedRows, totalRows, table.TableName, batchNumber);

                    batchNumber++;
                }

                offset += batchSize;
            }

            logger.LogInformation("Completed data migration for table: {TableName}", table.TableName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to migrate data for table {TableName}: {ErrorMessage}", table.TableName, ex.Message);

            throw;
        }
    }

    private async Task CreateIndexesAndForeignKeysAsync(TableInfo table)
    {
        try
        {
            // Generate and save index creation scripts
            var indexScripts = SqlServerService.GenerateIndexCreationScripts(table);
            await scriptGenerator.SaveIndexCreationScriptAsync(table, indexScripts);

            // Generate and save foreign key creation scripts
            var foreignKeyScripts = SqlServerService.GenerateForeignKeyCreationScripts(table);
            await scriptGenerator.SaveForeignKeyCreationScriptAsync(table, foreignKeyScripts);

            // Create indexes (excluding primary key indexes which are created with the table)
            await sqlServerService.CreateIndexesAsync(table);

            // Create foreign keys
            await sqlServerService.CreateForeignKeysAsync(table);

            logger.LogInformation("Created indexes and foreign keys for table: {TableName}", table.TableName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create indexes/foreign keys for table {TableName}: {ErrorMessage}",
                table.TableName, ex.Message);

            throw;
        }
    }

    private void DisplayTimingSummary(TimeSpan schemaCreationTime, TimeSpan dataMigrationTime, TimeSpan totalTime)
    {
        Console.WriteLine();
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine("MIGRATION TIMING SUMMARY");
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine($"Schema Creation Time: {FormatTimeSpan(schemaCreationTime)}");
        Console.WriteLine($"Data Migration Time:  {FormatTimeSpan(dataMigrationTime)}");
        Console.WriteLine($"Total Migration Time: {FormatTimeSpan(totalTime)}");
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine();

        logger.LogInformation("Migration timing summary:");
        logger.LogInformation("Schema Creation Time: {SchemaTime}", FormatTimeSpan(schemaCreationTime));
        logger.LogInformation("Data Migration Time: {DataTime}", FormatTimeSpan(dataMigrationTime));
        logger.LogInformation("Total Migration Time: {TotalTime}", FormatTimeSpan(totalTime));
    }

    private static string FormatTimeSpan(TimeSpan timeSpan)
    {
        if (timeSpan.TotalHours >= 1)
        {
            return $"{timeSpan.Hours:D2}:{timeSpan.Minutes:D2}:{timeSpan.Seconds:D2}.{timeSpan.Milliseconds:D3} (HH:MM:SS.mmm)";
        }

        return timeSpan.TotalMinutes >= 1 ? $"{timeSpan.Minutes:D2}:{timeSpan.Seconds:D2}.{timeSpan.Milliseconds:D3} (MM:SS.mmm)" : $"{timeSpan.Seconds}.{timeSpan.Milliseconds:D3} seconds";
    }

    private async Task DisableForeignKeyConstraintsAsync()
    {
        if (!disableForeignKeyConstraints)
        {
            logger.LogInformation("Skipping foreign key constraint disable (configured to keep constraints enabled)");
            return;
        }

        try
        {
            logger.LogInformation("Disabling foreign key constraints for data migration...");
            await sqlServerService.DisableForeignKeyConstraintsAsync();
            logger.LogInformation("Foreign key constraints disabled successfully");
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to disable foreign key constraints: {ErrorMessage}", ex.Message);
            logger.LogWarning("Continuing with data migration - foreign key violations may occur");
        }
    }

    private async Task EnableForeignKeyConstraintsAsync()
    {
        if (!disableForeignKeyConstraints)
        {
            logger.LogInformation("Skipping foreign key constraint re-enable (constraints were not disabled)");
            return;
        }

        try
        {
            logger.LogInformation("Re-enabling foreign key constraints...");
            await sqlServerService.EnableForeignKeyConstraintsAsync();
            logger.LogInformation("Foreign key constraints re-enabled successfully");
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to re-enable foreign key constraints: {ErrorMessage}", ex.Message);
        }
    }
}