namespace OneBrc
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    public class DavidHorrocksV2WeatherProcessor : IWeatherProcessor
    {
        /// <summary>
        /// Buffer size of one record.
        /// </summary>
        const int EstimateRecordLength = 1024;

        /// <summary>
        /// Must be bigger than EstimateRecordLength
        /// </summary>
        const long PreferredLargeFileChunkSize = 1024 * 1024;

        /// <summary>
        /// Reuse previously created station name strings
        /// </summary>
        ConcurrentDictionary<int, NameAndBytes>? StationNameLookUp = new ConcurrentDictionary<int, NameAndBytes>(Environment.ProcessorCount, 512);

        /// Aggregate station statistics from all threads.
        ConcurrentDictionary<string, StationData> AllStations = new ConcurrentDictionary<string, StationData>(Environment.ProcessorCount, 10000, StringComparer.OrdinalIgnoreCase);

        public IEnumerable<StationData> Process(string filePath)
        {
            FileInfo fi = new FileInfo(filePath);

            // The file position origin of where data begins.
            long origin = 0;

            // The UTF-8 BOM byte order mark.
            Span<byte> bomDefinition = [0xEF, 0xBB, 0xBF];
            Span<byte> bomCheck = [0, 0, 0];

            // Seek past the BOM and set a new origin.
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan))
            {
                int bytesRead = fs.Read(bomCheck);
                if (bytesRead == bomDefinition.Length)
                {
                    if (bomCheck.SequenceEqual(bomDefinition))
                    {
                        origin = bomDefinition.Length;
                    }
                }
            }

            long chunkSize;
            long chunkSizePerCpu = fi.Length / Environment.ProcessorCount;
            if (chunkSizePerCpu < EstimateRecordLength)
            {
                // Minimum chunk size.
                chunkSize = EstimateRecordLength;
            }
            else if (chunkSizePerCpu < PreferredLargeFileChunkSize)
            {
                // Ensure each CPU gets a chunk for small files.
                chunkSize = chunkSizePerCpu;
            }
            else
            {
                // Optimal chunk size.
                chunkSize = PreferredLargeFileChunkSize;
            }

            // Divide the file into chunks
            long fileChunkCount = (fi.Length - origin) / chunkSize;
            long lastChunkLength = (fi.Length - origin) % chunkSize;
            long fileAllChunkCount = fileChunkCount;
            if (lastChunkLength > 0)
            {
                // Include any remainder chunk.
                fileAllChunkCount++;
            }

            ParallelOptions options = new ParallelOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                CancellationToken = CancellationToken.None
            };

            Parallel.For(0L, fileAllChunkCount, options,
            () =>
            { 
                var data = new ThreadLocalData();
                data.ReadBuffer = new byte[chunkSize];
                data.RecordBuffer = new byte[EstimateRecordLength];
                data.fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
                data.Origin = origin;
                return data;
            }, 
            (i, loops, tVars) =>
            {
                if (tVars.fs != null && tVars.ReadBuffer != null && tVars.RecordBuffer != null && StationNameLookUp != null)
                {
                    Span<byte> buffer = tVars.RecordBuffer;
                    tVars.Curr = default;
                    tVars.Next = default;

                    // Locate the start of this chunk from within the file.
                    tVars.StartPosition = tVars.Origin + i * chunkSize;
                    tVars.PositionRead = 0;

                    // We typically will need to fetch on extra line because the reader of the next chunk will skip the line it find its self on at the start of its pass.
                    tVars.GiveMeExtraLines = 1;

                    // If true, tells the parser to triger a virtual EOF upon reading a new line.
                    tVars.FinishCurrentLine = false;

                    // See to the start if this chunk.
                    tVars.fs.Seek(tVars.StartPosition, SeekOrigin.Begin);
                    if (lastChunkLength == 0 || i + 1 < fileAllChunkCount)
                    {
                        tVars.BufferBytesRead = tVars.fs.Read(tVars.ReadBuffer, 0, tVars.ReadBuffer.Length);
                    }
                    else
                    {
                        // The last chunk may be the shortest chunk.
                         tVars.BufferBytesRead = tVars.fs.Read(tVars.ReadBuffer, 0, (int)lastChunkLength);
                    }

                    // One initial call to GetNextChar is needed to initialise the ReadField parser.
                    tVars.GetNextChar();
                    if (i != 0)
                    {
                        // If this is not the first chunk then we need to skip this line because the prevous chunk is read with one extra line.
                        while (!tVars.Next.EOF && tVars.Next.ch != (byte)'\n')
                        {
                            tVars.GetNextChar();
                        }
                    }

                    // Read until EOF
                    while (!tVars.Next.EOF)
                    {
                        bool isMoreData;
                        string? stationName;

                        // Read the station name
                        int stationNameLength = tVars.ReadField(buffer, 0, out isMoreData);
                        if (isMoreData)
                        {
                            // Grow the buffer as needed
                            buffer.CopyTo(tVars.RecordBuffer);
                            stationNameLength = tVars.ReadField(buffer, stationNameLength, out isMoreData);
                            buffer = new Span<byte>(tVars.RecordBuffer);
                        }

                        if (stationNameLength > 0)
                        {
                            NameAndBytes? nameAndBytes;
                            Span<byte> spanNameOfStation = buffer.Slice(0, stationNameLength);
                            int nameHash = GetHashCodeFromBytes(spanNameOfStation);
                            if (StationNameLookUp.TryGetValue(nameHash, out nameAndBytes))
                            {
                                // Try to reuse an existing string.
                                Span<byte> lhs = nameAndBytes.Bytes;
                                Span<byte> rhs = spanNameOfStation;
                                if (lhs.SequenceEqual(rhs))
                                {
                                    // Same hash and same string.
                                    stationName = nameAndBytes.Name ?? string.Empty;
                                }
                                else
                                {
                                    // Same hash but a different string.
                                    nameAndBytes = null;
                                }
                            }

                            if (nameAndBytes != null)
                            {
                                // We now reuse an existing string.
                                stationName = nameAndBytes.Name ?? string.Empty;
                            }
                            else
                            {
                                // Create a new string.
                                stationName = Encoding.UTF8.GetString(spanNameOfStation);
                                nameAndBytes = new NameAndBytes { Name = stationName, Bytes = spanNameOfStation.ToArray() };
                                StationNameLookUp.TryAdd(nameHash, nameAndBytes);
                            }

                            // Read the temperature
                            int tempLength = tVars.ReadField(buffer, 0, out isMoreData);
                            if (isMoreData)
                            {
                                // Grow the buffer as needed
                                buffer.CopyTo(tVars.RecordBuffer);
                                tempLength = tVars.ReadField(buffer, tempLength, out isMoreData);
                                buffer = new Span<byte>(tVars.RecordBuffer);
                            }

                            if (tempLength > 0)
                            {
                                // Each thread aggragates into its own private dictionary to minimise thread contention.
                                double tempValue = (double)ParseDecimal(buffer.Slice(0, tempLength));
                                StationData? stationData;
                                if (!tVars.Stations.TryGetValue(stationName, out stationData))
                                {
                                    stationData = new StationData()
                                    {
                                        Name = stationName,
                                        Min = tempValue,
                                        Max = tempValue,
                                        Count = 1,
                                        Sum = tempValue,
                                    };

                                    tVars.Stations[stationName] = stationData;
                                }
                                else
                                {
                                    stationData.Count++;
                                    stationData.Min = Math.Min(tempValue, stationData.Min);
                                    stationData.Max = Math.Max(tempValue, stationData.Max);
                                    stationData.Sum += tempValue;
                                }
                            }
                        }
                    }
                }

                return tVars;
            },
            (localOut) =>
            {
                if (localOut != null) 
                {
                    localOut.fs?.Dispose();

                    // Merge the dictionaries from each thread into the resulting dictionary.
                    foreach (var stationData in localOut.Stations)
                    {
                        AllStations.AddOrUpdate(stationData.Key, stationData.Value, (name, existingCity) =>
                        {
                            existingCity.Count += stationData.Value.Count;
                            existingCity.Min = Math.Min(existingCity.Min, stationData.Value.Min);
                            existingCity.Max = Math.Max(existingCity.Max, stationData.Value.Max);
                            existingCity.Sum += stationData.Value.Sum;
                            return existingCity;
                        });
                    }

                    localOut.Stations.Clear();
                }
            });

            return AllStations.Values.OrderBy(s => s.Name, StringComparer.Ordinal);
        }

        public static int GetHashCodeFromBytes(Span<byte> buffer)
        {
            unchecked
            {
                var result = buffer.Length;
                for (int i = 0; i < buffer.Length; i++)
                {
                    result = (result * 31) ^ buffer[i];
                }

                return result;
            }
        }

        public static int[] powof10 = new int[10]
        {
            1,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000
        };

        /// <summary>
        /// See https://stackoverflow.com/questions/37645870/faster-alternative-to-decimal-parse
        /// Credit to LukAss741 on Stack Overflow
        /// Parse a "string" ASCII bytes from a span of bytes into decimal.
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static decimal ParseDecimal(Span<byte> input)
        {
            int len = input.Length;
            if (len != 0)
            {
                bool negative = false;
                long n = 0;
                int start = 0;
                if (input[0] == '-')
                {
                    negative = true;
                    start = 1;
                }
                if (len <= 19)
                {
                    int decpos = len;
                    for (int k = start; k < len; k++)
                    {
                        byte c = input[k];
                        if (c == (byte)'.')
                        {
                            decpos = k + 1;
                        }
                        else
                        {
                            n = (n * 10) + (int)(c - '0');
                        }
                    }
                    return new decimal((int)n, (int)(n >> 32), 0, negative, (byte)(len - decpos));
                }
                else
                {
                    if (len > 28)
                    {
                        len = 28;
                    }
                    int decpos = len;
                    for (int k = start; k < 19; k++)
                    {
                        byte c = input[k];
                        if (c == (byte)'.')
                        {
                            decpos = k + 1;
                        }
                        else
                        {
                            n = (n * 10) + (int)(c - '0');
                        }
                    }
                    int n2 = 0;
                    bool secondhalfdec = false;
                    for (int k = 19; k < len; k++)
                    {
                        byte c = input[k];
                        if (c == (byte)'.')
                        {
                            decpos = k + 1;
                            secondhalfdec = true;
                        }
                        else
                        {
                            n2 = (n2 * 10) + (int)(c - '0');
                        }
                    }
                    byte decimalPosition = (byte)(len - decpos);
                    return new decimal((int)n, (int)(n >> 32), 0, negative, decimalPosition) * powof10[len - (!secondhalfdec ? 19 : 20)] + new decimal(n2, 0, 0, negative, decimalPosition);
                }
            }
            return 0;
        }

        /// <summary>
        /// Holds a char read by GetNextChar
        /// </summary>
        struct CurrentData
        {
            public byte ch;
            public bool EOF;
        }

        /// <summary>
        /// Used for pairing strings to a byte sequence to help with string resuse.
        /// </summary>
        class NameAndBytes
        {
            public string? Name;
            public byte[]? Bytes;
        }

        /// <summary>
        /// Thread local data to hold the state of a parsing a chunk of the file.
        /// </summary>
        class ThreadLocalData
        {
            public FileStream? fs;
            public byte[]? ReadBuffer;
            public int PositionRead;
            public long StartPosition;
            public int BufferBytesRead;
            public CurrentData Curr;
            public CurrentData Next;

            // Read extra lines beyond the current chunk.
            public int GiveMeExtraLines;
            public bool FinishCurrentLine;
            public byte[]? RecordBuffer;
            public long Origin;
            public Dictionary<string, StationData> Stations = new Dictionary<string, StationData>(10000, StringComparer.OrdinalIgnoreCase);

            /// <summary>
            /// Get one byte from the data.
            /// </summary>
            /// <returns>True if success, false if EOF is reached</returns>
            public bool GetNextChar()
            {
                unchecked
                {
                    // We maintain a one character look at head.
                    Curr = Next;
                    if (Curr.EOF || fs == null || ReadBuffer == null)
                    {
                        Next.EOF = true;
                        return false;
                    }

                    if (PositionRead >= BufferBytesRead)
                    {
                        // We reached the end of the current chunk.
                        if (GiveMeExtraLines > 0)
                        {
                            // Read extra lines beyond the current chunk.
                            if (!FinishCurrentLine)
                            {
                                if (Curr.ch == '\n')
                                {
                                    // Edge case where a newline still buffered from the previous chunk should not count towards the extra lines to read.
                                    ++GiveMeExtraLines;
                                }

                                // Disable checking for this edge case.
                                FinishCurrentLine = true;
                            }

                            // Read from another chunk.
                            PositionRead = 0;
                            BufferBytesRead = fs.Read(ReadBuffer, 0, EstimateRecordLength);
                            if (BufferBytesRead <= 0)
                            {
                                // File true EOF is reached.
                                Next.EOF = true;
                                return true;
                            }
                        }
                        else
                        {
                            // Chunk virtual EOF is reached.
                            Next.EOF = true;
                            return true;
                        }
                    }

                    Next.ch = ReadBuffer[PositionRead++];
                    if (FinishCurrentLine && Curr.ch == '\n')
                    {
                        if (--this.GiveMeExtraLines <= 0)
                        {
                            // The extra newlines are all read, so signal a virtual EOF.
                            Next.EOF = true;
                            FinishCurrentLine = false;
                        }
                    }

                    return true;
                }
            }

            /// <summary>
            /// Reads one field from the file, delimited  by either ; or \n or EOF
            /// Grows the buffer as needed.
            /// </summary>
            /// <param name="buffer"></param>
            /// <param name="startIndex"></param>
            /// <param name="isMoreData"></param>
            /// <returns>Returns the length of the field.</returns>
            public int ReadField(int startIndex)
            {
                int length = 0;
                if (RecordBuffer != null)
                {
                    length = ReadField(RecordBuffer, startIndex, out bool isMoreData);
                    while (isMoreData)
                    {
                        byte[] nextbuffer = new byte[RecordBuffer.Length * 2];
                        Buffer.BlockCopy(RecordBuffer, 0, nextbuffer, 0, RecordBuffer.Length);
                        RecordBuffer = nextbuffer;
                        length += ReadField(RecordBuffer, startIndex, out isMoreData);
                    }
                }

                return length;
            }

            /// <summary>
            /// Reads one field from the file, delimited  by either ; or \n or EOF
            /// </summary>
            /// <param name="buffer"></param>
            /// <param name="startIndex"></param>
            /// <param name="isMoreData"></param>
            /// <returns>Returns the length of the field.</returns>
            public int ReadField(Span<byte> buffer, int startIndex, out bool isMoreData)
            {
                isMoreData = false;
                startIndex = 0;
                while (true)
                {
                    if (startIndex >= buffer.Length)
                    {
                        isMoreData = true;
                        return startIndex;
                    }

                    if (!GetNextChar())
                    {
                        return startIndex;
                    }

                    if (Curr.EOF)
                    {
                        return startIndex;
                    }

                    if (Curr.ch > ';')
                    {
                        buffer[startIndex++] = Curr.ch;
                        continue;
                    }

                    switch (Curr.ch)
                    {
                        case (byte)';':
                            return startIndex;
                        case (byte)'\r':
                            if (Next.ch == (byte)'\n')
                            {
                                GetNextChar();
                                return startIndex;
                            }
                            else
                            {
                                buffer[startIndex++] = Curr.ch;
                            }

                            break;
                        case (byte)'\n':
                            return startIndex;
                        default:
                            buffer[startIndex++] = Curr.ch;
                            continue;
                    }
                }
            }
        }
    }
}