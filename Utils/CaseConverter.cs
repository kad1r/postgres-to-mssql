namespace PostgresToMsSqlMigration.Utils;

public static class CaseConverter
{
    /// <summary>
    /// Converts snake_case to PascalCase
    /// </summary>
    /// <param name="snakeCase">The snake_case string to convert</param>
    /// <returns>The PascalCase string</returns>
    public static string ToPascalCase(string snakeCase)
    {
        if (string.IsNullOrEmpty(snakeCase))
            return snakeCase;

        // Split by underscore and capitalize each part
        var parts = snakeCase.Split('_');
        var pascalCase = string.Join("", parts.Select(part => 
            part.Length > 0 ? char.ToUpper(part[0]) + part.Substring(1).ToLower() : ""));

        return pascalCase;
    }

    /// <summary>
    /// Converts PascalCase to snake_case
    /// </summary>
    /// <param name="pascalCase">The PascalCase string to convert</param>
    /// <returns>The snake_case string</returns>
    private static string ToSnakeCase(string pascalCase)
    {
        return string.IsNullOrEmpty(pascalCase) ? pascalCase : string.Concat(pascalCase.Select((x, i) => i > 0 && char.IsUpper(x) ? "_" + x.ToString() : x.ToString())).ToLower();
    }

    /// <summary>
    /// Converts a list of snake_case strings to PascalCase
    /// </summary>
    /// <param name="snakeCaseList">List of snake_case strings</param>
    /// <returns>List of PascalCase strings</returns>
    public static List<string> ToPascalCaseList(IEnumerable<string> snakeCaseList)
    {
        return snakeCaseList.Select(ToPascalCase).ToList();
    }

    /// <summary>
    /// Converts a list of PascalCase strings to snake_case
    /// </summary>
    /// <param name="pascalCaseList">List of PascalCase strings</param>
    /// <returns>List of snake_case strings</returns>
    public static List<string> ToSnakeCaseList(IEnumerable<string> pascalCaseList)
    {
        return pascalCaseList.Select(ToSnakeCase).ToList();
    }
} 