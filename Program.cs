using System.Collections;
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
        Hashtable results = new Hashtable();

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
            while (accessor.ReadByte(chunkEnd) != newLine)
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

        foreach (DictionaryEntry iter in results)
        {
            WeatherEntry v = (WeatherEntry)iter.Value;
            Console.WriteLine($"{iter.Key}: {v.min};{v.sum / v.cnt};{v.max}");
        }
        t0.Stop();
        Console.WriteLine($"Program run took: {t0.Elapsed}");
        foreach (Mapper m in mapperHandles)
        {
            Console.WriteLine(m.PrettyElapsed());
        }
    }

    static void Merge(Hashtable left, Hashtable right) // Merge right into left
    {
        foreach (DictionaryEntry iter in right)
        {
            string k = (string)iter.Key;
            WeatherEntry v = (WeatherEntry)iter.Value;
            if (left.Contains(k))
            {
                var lentry = (WeatherEntry)left[k];
                var rentry = (WeatherEntry)right[k];
                lentry.max = Math.Max(lentry.max, rentry.max);
                lentry.min = Math.Min(lentry.min, rentry.min);
                lentry.sum += rentry.sum;
                lentry.cnt += rentry.cnt;
            }
            else
            {
                left[k] = right[k];
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
    Hashtable _perThreadMap = new Hashtable();
    MemoryMappedViewAccessor _accessor;

    public Mapper(long id, MemoryMappedViewAccessor accessor)
    {
        _id = id;
        _accessor = accessor;
    }

    public Hashtable GetResults()
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
            if (_perThreadMap.Contains(name))
            {
                WeatherEntry entry = (WeatherEntry)_perThreadMap[name];
                entry.cnt++;
                entry.sum += temp;
                entry.min = Math.Min(entry.min, temp);
                entry.max = Math.Max(entry.max, temp);
            }
            else
            {
                WeatherEntry entry = new WeatherEntry();
                entry.cnt = 1;
                entry.sum = temp;
                entry.min = temp;
                entry.max = temp;
                _perThreadMap.Add(name, entry);
            }
        }
        t0.Stop();
        _ts = t0.Elapsed;
    }
}


