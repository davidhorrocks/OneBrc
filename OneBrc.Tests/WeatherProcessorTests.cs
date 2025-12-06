using System.Text;
using Xunit;
using OneBrc;

namespace OneBrc.Tests;

public class WeatherProcessorTests : IDisposable
{
    //private readonly IWeatherProcessor _processor = new NaiveWeatherProcessor();
    private readonly IWeatherProcessor _processor = new DavidHorrocksV2WeatherProcessor();
    private readonly List<string> _tempFiles = new();

    private string CreateTempFile(string content)
    {
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, content);
        _tempFiles.Add(tempFile);
        return tempFile;
    }

    public void Dispose()
    {
        foreach (var file in _tempFiles)
        {
            try { File.Delete(file); } catch { }
        }
    }

    [Fact]
    public void Process_SingleStation_SingleMeasurement()
    {
        var file = CreateTempFile("Hamburg;12.0");

        var results = _processor.Process(file).ToList();

        Assert.Single(results);
        var station = results[0];
        Assert.Equal("Hamburg", station.Name);
        Assert.Equal(12.0, station.Min);
        Assert.Equal(12.0, station.Mean);
        Assert.Equal(12.0, station.Max);
    }

    [Fact]
    public void Process_SingleStation_MultipleMeasurements()
    {
        var file = CreateTempFile("""
            Hamburg;12.0
            Hamburg;34.2
            Hamburg;8.9
            """);

        var results = _processor.Process(file).ToList();

        Assert.Single(results);
        var station = results[0];
        Assert.Equal("Hamburg", station.Name);
        Assert.Equal(8.9, station.Min);
        Assert.Equal(18.366666666666667, station.Mean, 10); // (12.0+34.2+8.9)/3
        Assert.Equal(34.2, station.Max);
    }

    [Fact]
    public void Process_MultipleStations_AlphabeticallySorted()
    {
        var file = CreateTempFile("""
            Hamburg;12.0
            Bulawayo;8.9
            Palembang;38.8
            Hamburg;34.2
            """);

        var results = _processor.Process(file).ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Bulawayo", results[0].Name);
        Assert.Equal("Hamburg", results[1].Name);
        Assert.Equal("Palembang", results[2].Name);
    }

    [Fact]
    public void Process_NegativeTemperatures()
    {
        var file = CreateTempFile("""
            Yakutsk;-25.3
            Yakutsk;-18.7
            Yakutsk;-32.1
            """);

        var results = _processor.Process(file).ToList();

        Assert.Single(results);
        var station = results[0];
        Assert.Equal("Yakutsk", station.Name);
        Assert.Equal(-32.1, station.Min);
        Assert.Equal(-25.366666666666667, station.Mean, 10); // (-25.3+-18.7+-32.1)/3
        Assert.Equal(-18.7, station.Max);
    }

    [Fact]
    public void Process_MixedPositiveNegativeTemperatures()
    {
        var file = CreateTempFile("""
            Berlin;-5.0
            Berlin;15.0
            Berlin;5.0
            """);

        var results = _processor.Process(file).ToList();

        Assert.Single(results);
        var station = results[0];
        Assert.Equal("Berlin", station.Name);
        Assert.Equal(-5.0, station.Min);
        Assert.Equal(5.0, station.Mean);
        Assert.Equal(15.0, station.Max);
    }

    [Fact]
    public void Process_ExampleFromChallenge()
    {
        var file = CreateTempFile("""
            Hamburg;12.0
            Bulawayo;8.9
            Palembang;38.8
            Hamburg;34.2
            St. John's;15.2
            Cracow;12.6
            """);

        var results = _processor.Process(file).ToList();

        Assert.Equal(5, results.Count);
        
        // Verify alphabetical order
        Assert.Equal("Bulawayo", results[0].Name);
        Assert.Equal("Cracow", results[1].Name);
        Assert.Equal("Hamburg", results[2].Name);
        Assert.Equal("Palembang", results[3].Name);
        Assert.Equal("St. John's", results[4].Name);
    }

    [Fact]
    public void Process_UTF8StationNames()
    {
        var file = CreateTempFile("""
            Zürich;9.3
            Abéché;29.4
            São Paulo;21.0
            """);

        var results = _processor.Process(file).ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, s => s.Name == "Abéché");
        Assert.Contains(results, s => s.Name == "São Paulo");
        Assert.Contains(results, s => s.Name == "Zürich");
    }

    [Fact]
    public void Process_MeanCalculation()
    {
        var file = CreateTempFile("""
            Test;10.0
            Test;20.0
            Test;30.0
            """);

        var results = _processor.Process(file).ToList();

        Assert.Single(results);
        var station = results[0];
        Assert.Equal(20.0, station.Mean);
    }

    [Fact]
    public void Process_BoundaryTemperatures()
    {
        var file = CreateTempFile("""
            Cold;-99.9
            Hot;99.9
            """);

        var results = _processor.Process(file).ToList();

        Assert.Equal(2, results.Count);
        
        var cold = results.First(s => s.Name == "Cold");
        Assert.Equal(-99.9, cold.Min);
        Assert.Equal(-99.9, cold.Max);
        
        var hot = results.First(s => s.Name == "Hot");
        Assert.Equal(99.9, hot.Min);
        Assert.Equal(99.9, hot.Max);
    }

    [Fact]
    public void Process_LargeNumberOfMeasurements()
    {
        var sb = new StringBuilder();
        for (int i = 0; i < 10000; i++)
        {
            sb.AppendLine($"Station;{(i % 100) / 10.0:F1}");
        }
        var file = CreateTempFile(sb.ToString());

        var results = _processor.Process(file).ToList();

        Assert.Single(results);
        var station = results[0];
        Assert.Equal("Station", station.Name);
        Assert.Equal(0.0, station.Min);
        Assert.Equal(9.9, station.Max);
    }

    [Fact]
    public void Process_FileBasedInput()
    {
        var file = CreateTempFile("""
            Hamburg;12.0
            Bulawayo;8.9
            Hamburg;34.2
            """);

        var results = _processor.Process(file).ToList();

        Assert.Equal(2, results.Count);
        
        var bulawayo = results.First(s => s.Name == "Bulawayo");
        Assert.Equal(8.9, bulawayo.Min);
        Assert.Equal(8.9, bulawayo.Mean);
        Assert.Equal(8.9, bulawayo.Max);
        
        var hamburg = results.First(s => s.Name == "Hamburg");
        Assert.Equal(12.0, hamburg.Min);
        Assert.Equal(23.1, hamburg.Mean);
        Assert.Equal(34.2, hamburg.Max);
    }
}
