using System;
using System.Collections.Concurrent;
using System.IO;

namespace MapReduceExample
{
    public class MapReduce
    {
        public MapReduce()
        {
            var fileStream = new FileStream("Input.txt", FileMode.Open);
            string text = "";
            using (StreamReader reader = new StreamReader(fileStream))
            {
                text = reader.ReadToEnd();
            }

            var dict = Run(text);
            foreach (var item in dict)
            {
                Console.WriteLine("Word: {0} Count: {1}", item.Key, item.Value);
            }
            Console.ReadLine();
        }

        private ConcurrentDictionary<string, int> Run(string text)
        {
            var dict = new ConcurrentDictionary<string, int>();

            text = text.Replace("\n", " ");
            text = text.Replace("\r", "");
            text = text.Replace("\t", " ");
            var words = text.Split(" ");

            foreach (var item in words)
            {
                dict.AddOrUpdate(item, 1, (id, count) => count + 1);
            }



            return dict;
        }
    }
}
