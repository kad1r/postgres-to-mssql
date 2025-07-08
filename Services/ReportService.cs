using System.Text.Json;
using PostgresToMsSqlMigration.Models;
using PostgresToMsSqlMigration.Utils;
using Microsoft.Extensions.Logging;

namespace PostgresToMsSqlMigration.Services;

public class ReportService
{
    private readonly ILogger<ReportService> _logger;
    private readonly MigrationReport _report;

    public ReportService(ILogger<ReportService> logger)
    {
        _logger = logger;
        _report = new MigrationReport();
    }

    public void AddTableRenameInfo(TableInfo table)
    {
        var tableRenameInfo = new ColumnRenameInfo
        {
            TableName = table.TableName,
            SchemaName = table.SchemaName,
            RenamedColumns = new List<ColumnRename>()
        };

        foreach (var column in table.Columns)
        {
            var originalName = column.ColumnName;
            var pascalCaseName = CaseConverter.ToPascalCase(originalName);
            var finalName = ReservedKeywordHandler.EscapeIdentifier(pascalCaseName);
            
            var reason = "";
            if (originalName != pascalCaseName)
            {
                reason = "Case conversion (snake_case to PascalCase)";
            }
            if (ReservedKeywordHandler.IsReservedKeyword(pascalCaseName))
            {
                reason += reason.Length > 0 ? "; Reserved keyword escaped" : "Reserved keyword escaped";
            }

            if (originalName != finalName)
            {
                tableRenameInfo.RenamedColumns.Add(new ColumnRename
                {
                    OriginalName = originalName,
                    PascalCaseName = pascalCaseName,
                    FinalName = finalName,
                    Reason = reason
                });
            }
        }

        if (tableRenameInfo.RenamedColumns.Any())
        {
            _report.Tables.Add(tableRenameInfo);
            _logger.LogInformation("Table {TableName}: {Count} columns renamed", 
                table.TableName, tableRenameInfo.RenamedColumns.Count);
        }

        // Track index renames
        var indexRenameInfo = new IndexRenameInfo
        {
            TableName = table.TableName,
            SchemaName = table.SchemaName,
            RenamedIndexes = new List<IndexRename>()
        };

        foreach (var index in table.Indexes)
        {
            if (!string.IsNullOrEmpty(index.OriginalIndexName) && index.OriginalIndexName != index.IndexName)
            {
                var reason = "Invalid characters replaced";
                if (IndexNameCleaner.NeedsCleaning(index.OriginalIndexName))
                {
                    reason = "Invalid characters (like ~) replaced with underscores";
                }

                indexRenameInfo.RenamedIndexes.Add(new IndexRename
                {
                    OriginalName = index.OriginalIndexName,
                    CleanedName = index.IndexName,
                    Reason = reason
                });
            }
        }

        if (indexRenameInfo.RenamedIndexes.Any())
        {
            _report.IndexRenames.Add(indexRenameInfo);
            _logger.LogInformation("Table {TableName}: {Count} indexes renamed", 
                table.TableName, indexRenameInfo.RenamedIndexes.Count);
        }
    }

    public async Task SaveReportAsync(string outputPath = "files/migration_report.json")
    {
        _report.TotalTables = _report.Tables.Count;
        _report.TotalRenamedColumns = _report.Tables.Sum(t => t.RenamedColumns.Count);
        _report.TotalRenamedIndexes = _report.IndexRenames.Sum(i => i.RenamedIndexes.Count);

        // Ensure the files directory exists
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        var json = JsonSerializer.Serialize(_report, options);
        await File.WriteAllTextAsync(outputPath, json);

        _logger.LogInformation("Migration report saved to: {OutputPath}", outputPath);
        _logger.LogInformation("Total tables processed: {TotalTables}", _report.TotalTables);
        _logger.LogInformation("Total columns renamed: {TotalRenamedColumns}", _report.TotalRenamedColumns);
        _logger.LogInformation("Total indexes renamed: {TotalRenamedIndexes}", _report.TotalRenamedIndexes);
    }

    public MigrationReport GetReport()
    {
        return _report;
    }
} 