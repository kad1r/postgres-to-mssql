using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PostgresToMsSqlMigration.Services;
using PostgresToMsSqlMigration.Models;
using PostgresToMsSqlMigration.Utils;

namespace PostgresToMsSqlMigration;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            // Build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            // Setup dependency injection
            var services = new ServiceCollection();

            // Add logging
            services.AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Information);
            });

            // Add configuration
            services.AddSingleton<IConfiguration>(configuration);

            // Add services
            services.AddSingleton<PostgreSqlService>(provider =>
            {
                var config = provider.GetRequiredService<IConfiguration>();
                var logger = provider.GetRequiredService<ILogger<PostgreSqlService>>();
                var connectionString = config.GetConnectionString("PostgreSQL") 
                    ?? throw new InvalidOperationException("PostgreSQL connection string not found");
                return new PostgreSqlService(connectionString, logger);
            });

            services.AddSingleton<SqlServerService>(provider =>
            {
                var config = provider.GetRequiredService<IConfiguration>();
                var logger = provider.GetRequiredService<ILogger<SqlServerService>>();
                var connectionString = config.GetConnectionString("SqlServer") 
                    ?? throw new InvalidOperationException("SQL Server connection string not found");
                var enableIdentityInsert = config.GetValue<bool>("MigrationSettings:EnableIdentityInsert", true);
                return new SqlServerService(connectionString, logger, enableIdentityInsert);
            });

            services.AddSingleton<ReportService>(provider =>
            {
                var logger = provider.GetRequiredService<ILogger<ReportService>>();
                return new ReportService(logger);
            });

            services.AddSingleton<SqlScriptGenerator>(provider =>
            {
                var config = provider.GetRequiredService<IConfiguration>();
                var logger = provider.GetRequiredService<ILogger<SqlScriptGenerator>>();
                var enableIdentityInsert = config.GetValue<bool>("MigrationSettings:EnableIdentityInsert", true);
                return new SqlScriptGenerator(logger, enableIdentityInsert);
            });

            services.AddSingleton<DateValidator>(provider =>
            {
                var logger = provider.GetRequiredService<ILogger<DateValidator>>();
                return new DateValidator(logger);
            });

            services.AddSingleton<MigrationService>(provider =>
            {
                var postgreService = provider.GetRequiredService<PostgreSqlService>();
                var sqlService = provider.GetRequiredService<SqlServerService>();
                var reportService = provider.GetRequiredService<ReportService>();
                var scriptGenerator = provider.GetRequiredService<SqlScriptGenerator>();
                var dateValidator = provider.GetRequiredService<DateValidator>();
                var logger = provider.GetRequiredService<ILogger<MigrationService>>();
                var config = provider.GetRequiredService<IConfiguration>();
                var batchSize = config.GetValue<int>("MigrationSettings:BatchSize", 1000);
                var scriptGenerationMethod = config.GetValue<string>("MigrationSettings:ScriptGenerationMethod", "Optimized");
                var useBulkInsert = config.GetValue<bool>("MigrationSettings:UseBulkInsert", false);
                var disableForeignKeyConstraints = config.GetValue<bool>("MigrationSettings:DisableForeignKeyConstraints", true);
                var validateScriptsForDuplicates = config.GetValue<bool>("MigrationSettings:ValidateScriptsForDuplicates", true);
                return new MigrationService(postgreService, sqlService, reportService, scriptGenerator, dateValidator, logger, batchSize, scriptGenerationMethod, useBulkInsert, disableForeignKeyConstraints, validateScriptsForDuplicates);
            });

            var serviceProvider = services.BuildServiceProvider();

            // Get migration service and run migration
            var migrationService = serviceProvider.GetRequiredService<MigrationService>();
            var logger = serviceProvider.GetRequiredService<ILogger<Program>>();

            // Get skip tables from configuration
            var skipTables = configuration.GetSection("MigrationSettings:SkipTables")
                .Get<List<string>>() ?? new List<string>();
            var skipDataMigrationTables = configuration.GetSection("MigrationSettings:SkipDataMigrationTables")
                .Get<List<string>>() ?? new List<string>();

            logger.LogInformation("Starting PostgreSQL to SQL Server Migration Tool");
            logger.LogInformation("Skip tables (schema): {SkipTables}", string.Join(", ", skipTables));
            logger.LogInformation("Skip tables (data): {SkipDataTables}", string.Join(", ", skipDataMigrationTables));

            // Display menu and get user choice
            var migrationMode = DisplayMenuAndGetChoice();

            // Run the migration
            await migrationService.MigrateAsync(skipTables, skipDataMigrationTables, migrationMode);

            logger.LogInformation("Migration completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Migration failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            Environment.Exit(1);
        }
    }

    private static MigrationMode DisplayMenuAndGetChoice()
    {
        Console.WriteLine();
        Console.WriteLine("=".PadRight(50, '='));
        Console.WriteLine("POSTGRESQL TO SQL SERVER MIGRATION TOOL");
        Console.WriteLine("=".PadRight(50, '='));
        Console.WriteLine();
        Console.WriteLine("Please select migration mode:");
        Console.WriteLine("1. Schema Only - Create tables, indexes, and foreign keys");
        Console.WriteLine("2. Data Only - Migrate data only (tables must exist)");
        Console.WriteLine("3. Both - Complete migration (schema + data)");
        Console.WriteLine("4. Data Scripts Only - Generate data migration scripts without execution");
        Console.WriteLine();
        Console.Write("Enter your choice (1-4): ");

        while (true)
        {
            var input = Console.ReadLine()?.Trim();
            
            if (int.TryParse(input, out var choice))
            {
                switch (choice)
                {
                    case 1:
                        Console.WriteLine("Selected: Schema Only migration");
                        return MigrationMode.SchemaOnly;
                    case 2:
                        Console.WriteLine("Selected: Data Only migration");
                        return MigrationMode.DataOnly;
                    case 3:
                        Console.WriteLine("Selected: Complete migration (Schema + Data)");
                        return MigrationMode.Both;
                    case 4:
                        Console.WriteLine("Selected: Data Scripts Only - Generate scripts without execution");
                        return MigrationMode.DataScriptsOnly;
                    default:
                        Console.Write("Invalid choice. Please enter 1, 2, 3, or 4: ");
                        break;
                }
            }
            else
            {
                Console.Write("Invalid input. Please enter a number (1-3): ");
            }
        }
    }
} 