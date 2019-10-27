using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace MapReduceExample
{
    public class MapReduce
    {
        const string inputFile = "Input.txt";
        public MapReduce()
        {
            while (true)
            {
                Console.Write("Running parallel...");
                Stopwatch sw = Stopwatch.StartNew();
                var dictPara = RunParallel(inputFile);
                sw.Stop();
                Console.WriteLine(" Done.");
                Console.WriteLine("Parallel time: {0:F6}", (sw.ElapsedMilliseconds / 1000.0));
                Console.Write("Running sequential...");
                sw.Restart();
                var dictSeq = RunSequential(inputFile);
                sw.Stop();
                Console.WriteLine(" Done.");

                Console.WriteLine("Sequential time: {0:F6}", (sw.ElapsedMilliseconds / 1000.0));

                Console.WriteLine("Parallel results:");
                print(dictPara);
                Console.WriteLine("");
                Console.WriteLine("Sequential results:");
                print(dictSeq);

                Console.ReadLine();
                Console.Clear();
            }
        }

        private void print(ConcurrentDictionary<string, int> dict)
        {
            foreach (var item in dict)
            {
                Console.WriteLine("Word: {0} Count: {1}", item.Key, item.Value);
            }
        }

        private ConcurrentDictionary<string, int> RunSequential(string file)
        {
            var fileStream = new FileStream(file, FileMode.Open);
            string text = "";
            using (StreamReader reader = new StreamReader(fileStream))
            {
                text = reader.ReadToEnd();
            }
            return CountWords(text, new ConcurrentDictionary<string, int>());
        }

        private ConcurrentDictionary<string, int> RunParallel(string file)
        {
            var dict = new ConcurrentDictionary<string, int>();
            Parallel.ForEach(
                File.ReadLines(file),
                new ParallelOptions { },
                () => new ConcurrentDictionary<string, int>(),
                (line, state, lineNumber, tempDict) =>
            {
                CountWords(line, tempDict);
                return tempDict;
            },
                tempDict =>
                {
                    foreach (var item in tempDict)
                    {
                        if (dict.ContainsKey(item.Key))
                        {
                            dict[item.Key] += item.Value;
                        }
                        else
                        {
                            dict[item.Key] = item.Value;
                        }

                    }
                });
            return dict;
        }

        private ConcurrentDictionary<string, int> CountWords(string text, ConcurrentDictionary<string, int> dict)
        {
            text = text.Replace("\n", " ");
            text = text.Replace("\r", "");
            text = text.Replace("\t", " ");
            var words = text.Split(" ", StringSplitOptions.RemoveEmptyEntries);

            foreach (var item in words)
            {
                if (dict.ContainsKey(item))
                {
                    dict[item] += 1;
                }
                else
                {
                    dict[item] = 1;
                }
            }

            return dict;
        }
    }
}
