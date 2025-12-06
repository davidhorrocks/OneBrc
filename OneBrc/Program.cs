using System.Diagnostics;
using System.Globalization;
using OneBrc;

if (args.Length < 1)
{
    PrintUsage();
    return 0;
}

var command = args[0].ToLowerInvariant();

// Use pattern matching with switch expression for command routing
var result = command switch
{
    "generate" or "gen" or "g" => HandleGenerate(args),
    "process" or "proc" or "p" => HandleProcess(args),
    "help" or "-h" or "--help" or "?" => HandleHelp(),
    _ => HandleUnknownCommand(command)
};

return result;

static int HandleGenerate(string[] args)
{
    if (args.Length < 2 || !int.TryParse(args[1], out int size) || size <= 0)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("Error: Please provide a valid number of records to generate.");
        Console.ResetColor();
        Console.WriteLine();
        Console.WriteLine("Usage: OneBrc generate <number of records> [output file]");
        Console.WriteLine();
        Console.WriteLine("Arguments:");
        Console.WriteLine("  <number of records>  Number of measurement records to generate (required)");
        Console.WriteLine("  [output file]        Output file path (default: measurements.txt)");
        Console.WriteLine();
        Console.WriteLine("Examples:");
        Console.WriteLine("  dotnet run -- generate 1000000");
        Console.WriteLine("  dotnet run -- gen 1000000 mydata.txt");
        Console.WriteLine("  dotnet run -- g 1000000000   # 1 billion records");
        return 1;
    }
    
    var outputFile = args.Length >= 3 ? args[2] : "measurements.txt";
    WeatherGenerator.GenerateMeasurements(size, outputFile);
    return 0;
}

static int HandleProcess(string[] args)
{
    var inputFile = args.Length >= 2 ? args[1] : "measurements.txt";
    
    if (!File.Exists(inputFile))
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"Error: File not found: {inputFile}");
        Console.ResetColor();
        return 1;
    }
    
    Console.WriteLine($"Processing {inputFile}...");
    
    // Force GC and get baseline
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    
    var heapBefore = GC.GetAllocatedBytesForCurrentThread();
    var (gen0Before, gen1Before, gen2Before) = (
        GC.CollectionCount(0),
        GC.CollectionCount(1),
        GC.CollectionCount(2)
    );
    
    var startTime = Stopwatch.GetTimestamp();
    //IWeatherProcessor processor = new NaiveWeatherProcessor();
    IWeatherProcessor processor = new DavidHorrocksV2WeatherProcessor();
    var results = processor.Process(inputFile).ToList(); // Materialize to capture all allocations
    var elapsed = Stopwatch.GetElapsedTime(startTime);
    
    var heapAfter = GC.GetAllocatedBytesForCurrentThread();
    var (gen0After, gen1After, gen2After) = (
        GC.CollectionCount(0),
        GC.CollectionCount(1),
        GC.CollectionCount(2)
    );
    
    Console.WriteLine();
    foreach (var station in results)
    {
        var minStr = station.Min.ToString("F1", CultureInfo.InvariantCulture);
        var meanStr = station.Mean.ToString("F1", CultureInfo.InvariantCulture);
        var maxStr = station.Max.ToString("F1", CultureInfo.InvariantCulture);
        Console.WriteLine($"{station.Name};{minStr};{meanStr};{maxStr}");
    }
    
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"✓ Processed in {elapsed.TotalMilliseconds :N0} ms");
    Console.ResetColor();
    
    // Memory stats
    var heapAllocated = heapAfter - heapBefore;
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine("Memory Statistics:");
    Console.ResetColor();
    Console.WriteLine($"  Heap allocated:    {FormatBytes(heapAllocated)}");
    Console.WriteLine($"  GC Collections:    Gen0={gen0After - gen0Before}, Gen1={gen1After - gen1Before}, Gen2={gen2After - gen2Before}");
    Console.WriteLine($"  Total GC Memory:   {FormatBytes(GC.GetTotalMemory(false))}");
    
    return 0;
}

static int HandleHelp()
{
    PrintUsage();
    return 0;
}

static int HandleUnknownCommand(string command)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"Error: Unknown command '{command}'");
    Console.ResetColor();
    Console.WriteLine();
    PrintUsage();
    return 1;
}

static void PrintUsage()
{
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║              1 Billion Row Challenge (1BRC) - C#             ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");
    Console.ResetColor();
    Console.WriteLine();
    Console.WriteLine("DESCRIPTION:");
    Console.WriteLine("  Generate weather measurement data and calculate min/mean/max");
    Console.WriteLine("  temperature statistics per weather station.");
    Console.WriteLine();
    Console.WriteLine("COMMANDS:");
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.Write("  generate, gen, g ");
    Console.ResetColor();
    Console.WriteLine("<count> [file]  Generate measurement data");
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.Write("  process, proc, p ");
    Console.ResetColor();
    Console.WriteLine("[file]          Process measurements file");
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.Write("  help, -h, --help ");
    Console.ResetColor();
    Console.WriteLine("                Show this help message");
    Console.WriteLine();
    Console.WriteLine("EXAMPLES:");
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.WriteLine("  # Generate 1 million measurements");
    Console.ResetColor();
    Console.WriteLine("  dotnet run -- generate 1000000");
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.WriteLine("  # Generate 1 billion measurements to custom file");
    Console.ResetColor();
    Console.WriteLine("  dotnet run -- gen 1000000000 bigdata.txt");
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.WriteLine("  # Process the default measurements.txt file");
    Console.ResetColor();
    Console.WriteLine("  dotnet run -- process");
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.WriteLine("  # Process a custom file");
    Console.ResetColor();
    Console.WriteLine("  dotnet run -- process mydata.txt");
    Console.WriteLine();
}

static string FormatBytes(long bytes)
{
    ReadOnlySpan<string> suffixes = ["B", "KB", "MB", "GB"];
    int suffixIndex = 0;
    double size = bytes;
    
    while (size >= 1024 && suffixIndex < suffixes.Length - 1)
    {
        size /= 1024;
        suffixIndex++;
    }
    
    return $"{size:N2} {suffixes[suffixIndex]}";
}

