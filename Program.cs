using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

class Program
{
    unsafe static void Main()
    {
        int nproc = 10; //Environment.ProcessorCount;
        Stopwatch t0 = Stopwatch.StartNew();
        string filePath = "../measurements.txt";
        long oz = 0x0;
        long fz = 0x3b4cf0fa8;
        long cz = fz / nproc;
        var threadHandles = (new Thread[nproc]).AsSpan();
        var mapperHandles = (new Mapper[nproc]).AsSpan();
        Dictionary<string, WeatherEntry> results = new Dictionary<string, WeatherEntry>();

        using var mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, "measurements");
        using var accessor = mmf.CreateViewAccessor((long)oz, (long)fz);

#if DEBUG
        Console.WriteLine($"Accessor capacity, ptr offset: {accessor.Capacity}, {accessor.PointerOffset}");
#endif
        long chunkStart = 0;
        byte newLine = ((byte)'\n');
        for (var i = 0; i < threadHandles.Length; i++)
        {
            long chunkEnd = Math.Min(chunkStart + cz, fz - 1); // Don't include EOF
            byte* mmPtr = null;
            while (accessor.ReadByte(chunkEnd) != newLine && chunkEnd < fz)
            {
                chunkEnd++;
            }
            var view = mmf.CreateViewAccessor(chunkStart, chunkEnd - chunkStart, MemoryMappedFileAccess.Read);
            mapperHandles[i] = new Mapper(i, view);
            threadHandles[i] = new Thread(new ThreadStart(mapperHandles[i].Accumulate));
            threadHandles[i].Start();
#if DEBUG
            Console.WriteLine($"Chunk {i}; Start: {chunkStart}, End: {chunkEnd}");
            Console.WriteLine($"(char)StartByte: {(char)accessor.ReadByte(chunkStart)}");
            Console.WriteLine($"(char)EndByte: {(char)accessor.ReadByte(chunkEnd)}");
#endif
            chunkStart = chunkEnd + 1;
        }

        for (var i = 0; i < threadHandles.Length; i++)
        {
            threadHandles[i].Join();
            var m = mapperHandles[i];
            Merge(results, m.GetResults());
        }
        foreach (KeyValuePair<string, WeatherEntry> iter in results)
        {
            var v = iter.Value;
            Console.WriteLine($"{iter.Key}: {v.min};{v.sum / v.cnt};{v.max}");
        }
        t0.Stop();
        Console.WriteLine($"Program run took: {t0.Elapsed}");
        foreach (Mapper m in mapperHandles)
        {
            Console.WriteLine(m.PrettyElapsed());
        }
    }

    static void Merge(Dictionary<string, WeatherEntry> left, Dictionary<string, WeatherEntry> right) // Merge right into left
    {
        foreach (KeyValuePair<string, WeatherEntry> iter in right)
        {
            WeatherEntry lentry;
            WeatherEntry rentry = iter.Value;
            if (left.TryGetValue(iter.Key, out lentry))
            {
                lentry.max = Math.Max(lentry.max, rentry.max);
                lentry.min = Math.Min(lentry.min, rentry.min);
                lentry.sum += rentry.sum;
                lentry.cnt += rentry.cnt;
                left[iter.Key] = lentry;
            }
            else
            {
                left.Add(iter.Key, iter.Value);
            }
        }
    }
}

struct WeatherEntry
{
    public float min;
    public float sum;
    public float max;
    public Int32 cnt;
}

class Mapper
{
    TimeSpan _ts = TimeSpan.Zero;
    long _id;
    Dictionary<string, WeatherEntry> _perThreadMap = new Dictionary<string, WeatherEntry>();
    MemoryMappedViewAccessor _accessor;

    public Mapper(long id, MemoryMappedViewAccessor accessor)
    {
        _id = id;
        _accessor = accessor;
    }

    public Dictionary<string, WeatherEntry> GetResults()
    {
        return _perThreadMap;
    }

    public string PrettyElapsed()
    {
        return $"Thread {_id} elapsed: {_ts}";
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public unsafe void Accumulate()
    {
        var t0 = Stopwatch.StartNew();
        var BUFSIZE = 64;
        var cap = _accessor.Capacity;
        var viewHandle = _accessor.SafeMemoryMappedViewHandle;
        byte* handle = null;
        viewHandle.AcquirePointer(ref handle);

        var nameBuf = stackalloc byte[BUFSIZE];
        var tempBuf = stackalloc byte[BUFSIZE];

        Int32 idx = 0;
        while (idx < cap)
        {
            Int32 cur = 0;
            while (true)
            {
                byte c = *(handle + idx);
                if (c == (byte)';')
                {
                    idx++;
                    break;
                }
#if DEBUG
                if (cur > BUFSIZE)
                {
                    Console.WriteLine($"Name Buffer overflowww {cur} > {BUFSIZE}");
                }
#endif
                nameBuf[cur] = c;
                cur++;
                idx++;
            }
            string name = System.Text.Encoding.UTF8.GetString(nameBuf, cur);

            cur = 0;

            while (true)
            {
                byte c = *(handle + idx);
                if (c == (byte)'\n')
                {
                    idx++;
                    break;
                }
                tempBuf[cur] = c;
#if DEBUG
                if (cur > BUFSIZE)
                {
                    Console.WriteLine($"Temp Buffer overflowww {cur} > {BUFSIZE}");
                }
#endif
                idx++;
                cur++;
            }
            var strT = System.Text.Encoding.UTF8.GetString(tempBuf, cur);
            var temp = Convert.ToSingle(strT);
            WeatherEntry entry;
            if (_perThreadMap.TryGetValue(name, out entry))
            {
                entry.cnt += 1;
                entry.sum += temp;
                entry.min = Math.Min(entry.min, temp);
                entry.max = Math.Max(entry.max, temp);
            }
            else
            {
                WeatherEntry newEntry = new WeatherEntry
                {
                    cnt = 1,
                    sum = temp,
                    min = temp,
                    max = temp
                };
                _perThreadMap.Add(name, newEntry);
            }
        }
        viewHandle.ReleasePointer();
        t0.Stop();
        _ts = t0.Elapsed;
    }
}


