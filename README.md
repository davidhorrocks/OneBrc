# 1️⃣🐝🏎️ One Billion Row Challenge - C# Implementation

A C# implementation of the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc) (1BRC), originally created by Gunnar Morling.

## The Challenge

The One Billion Row Challenge (1BRC) is a fun exploration of how far modern programming languages can be pushed for aggregating one billion rows from a text file.

### Task Description

Given a text file containing temperature measurements from various weather stations, calculate the **min**, **mean**, and **max** temperature per weather station.

**Input format:**
```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
...
```

Each row contains:
- Station name: UTF-8 string (1-100 bytes)
- Semicolon separator
- Temperature: Value between -99.9 and 99.9 with one fractional digit

**Expected output:**
```
Bulawayo;8.9;22.1;35.2
Cracow;12.6;12.6;12.6
Hamburg;12.0;23.1;34.2
...
```

Results are sorted alphabetically by station name, with min, mean, and max values rounded to one decimal place.

## Getting Started

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download) or later

### Building

```bash
cd OneBrc
dotnet build
```

### Running

The application supports two main commands:

#### Generate Measurement Data

Create a test file with random weather measurements:

```bash
# Generate 1 million measurements (default output: measurements.txt)
dotnet run -- generate 1000000

# Generate 1 billion measurements to a custom file
dotnet run -- gen 1000000000 bigdata.txt

# Short form
dotnet run -- g 1000000
```

#### Process Measurements

Calculate min/mean/max statistics per weather station:

```bash
# Process the default measurements.txt file
dotnet run -- process

# Process a specific file
dotnet run -- process mydata.txt

# Short form
dotnet run -- p measurements.txt
```

The processor outputs timing and memory statistics:
- **Heap allocated** - Total bytes allocated on the managed heap
- **GC Collections** - Garbage collections per generation (Gen0/Gen1/Gen2)
- **Total GC Memory** - Current managed heap size after processing

#### Help

```bash
dotnet run -- help
dotnet run -- -h
dotnet run -- --help
```

## Project Structure

```
OneBrc/
├── OneBrc/
│   ├── Program.cs              # CLI entry point
│   ├── IWeatherProcessor.cs    # Processor interface
│   ├── WeatherProcessor.cs     # Naive implementation
│   ├── StationData.cs          # Station data model
│   ├── WeatherGenerator.cs     # Test data generation
│   └── OneBrc.csproj
├── OneBrc.Tests/
│   ├── WeatherProcessorTests.cs  # Unit tests
│   └── OneBrc.Tests.csproj
├── docs/
│   └── MODERN_CSHARP_FEATURES.md  # Modern C# features documentation
├── OneBrc.slnx
└── README.md
```

> 📘 **Note:** This project uses modern C# 13 and .NET 10 features. See [Modern C# Features Documentation](docs/MODERN_CSHARP_FEATURES.md) for details on the language features used.

## Implementation Details

### Architecture

The project uses an interface-based design to enable engineers to implement optimized processors:

```csharp
public interface IWeatherProcessor
{
    IEnumerable<StationData> Process(string filePath);
}
```

**`StationData`** contains the aggregated results:
- `Name` - Station name
- `Min`, `Max` - Temperature extremes
- `Sum`, `Count` - For calculating mean
- `Mean` - Computed average temperature

### Reference Implementation (Naive)

`NaiveWeatherProcessor` provides a baseline implementation:

1. Read the file line by line using `File.ReadLines()`
2. Parse each line to extract station name and temperature
3. Use a `Dictionary<string, StationData>` to track min, max, sum, and count per station
4. Return results sorted alphabetically by station name

This serves as a correctness baseline. Engineers should create optimized implementations of `IWeatherProcessor`.

### Creating Your Own Implementation

To create an optimized implementation:

1. Create a new class implementing `IWeatherProcessor`
2. Implement `Process(string filePath)` using your optimization strategy
3. Return `IEnumerable<StationData>` sorted alphabetically by station name
4. Run existing tests against your implementation to verify correctness

Example strategies:
- Memory-mapped files
- Parallel chunk processing
- SIMD parsing
- Custom hash maps
- Span-based zero-allocation parsing

### Data Generation

The generator creates realistic weather data:
- **413 weather stations** from around the world (same as the original Java implementation)
- Temperatures follow a **Gaussian distribution** around each station's mean temperature
- Standard deviation of 10°C for realistic variation

## Rules (from the original challenge)

- No external library dependencies (standard library only)
- Single source file implementation (for competition submissions)
- Computation must happen at runtime (no build-time processing)
- Must support up to 10,000 unique station names
- Must handle any valid UTF-8 station name
- Implementation must not rely on specifics of a given data set

## Running Tests

```bash
cd OneBrc
dotnet test
```

## Performance

| Implementation | Time (1B rows) | Notes |
|---------------|----------------|-------|
| Naive (current) | TBD | Baseline implementation |

*Benchmarks run on: [Your machine specs here]*

## Optimization Ideas

Potential optimizations to explore when implementing `IWeatherProcessor`:

- [ ] Memory-mapped file I/O
- [ ] Parallel processing with multiple threads
- [ ] Custom parsing (avoid string allocations)
- [ ] SIMD operations for parsing
- [ ] Custom hash map implementation
- [ ] Span<T> and stackalloc for zero-allocation parsing
- [ ] ReadOnlySpan<byte> to avoid UTF-8 decoding overhead

## Resources

### Original Challenge
- [1BRC GitHub Repository](https://github.com/gunnarmorling/1brc)
- [Challenge Announcement Blog Post](https://www.morling.dev/blog/one-billion-row-challenge/)
- [Results & Winners](https://www.morling.dev/blog/1brc-results-are-in/)

### Related Implementations & Blog Posts
- [1BRC in .NET - Optimization Journey](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/) by Victor Baybekov
- [The One Billion Row Challenge - .NET Edition](https://dev.to/mergeconflict/392-the-one-billion-row-challenge-net-edition) (podcast)
- [One Billion Row Challenge in Golang](https://www.bytesizego.com/blog/one-billion-row-challenge-go) by Shraddha Agrawal

## License

This project is available under the MIT Licence.

## Acknowledgments

- [Gunnar Morling](https://github.com/gunnarmorling) for creating the original One Billion Row Challenge
- All contributors to the original 1BRC challenge for inspiration and optimization techniques
