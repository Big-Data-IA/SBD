using System;
using System.IO;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace WordCountSpark
{
    class Program
    {
        static string GetRandomString(int length)
        {
            var random = new Random();
            const string chars = "abcdefghijklmnopqrstuvwxyz";
            return new string(Enumerable.Range(0, length)
                                        .Select(x => chars[random.Next(chars.Length)])
                                        .ToArray());
        }

        static void Main(string[] args)
        {
            string filename = args.Length > 0 ? args[0] : "/opt/spark-data/random_text.txt";

            // Create Spark session
            var spark = SparkSession.Builder()
                                    .AppName("WordCountCSharp")
                                    .GetOrCreate();

            // Read file as DataFrame
            DataFrame df = spark.Read().Text(filename);

            // Split lines into words, explode to get one word per row
            DataFrame wordsDf = df.Select(Explode(Split(Col("value"), " ")).Alias("word"));

            // Group by word and count
            DataFrame wordCounts = wordsDf.GroupBy("word").Count();

            // Generate random output folder
            string outputPath = $"/opt/spark-data/{GetRandomString(8)}";
            wordCounts.Write().Mode(SaveMode.Overwrite).Csv(outputPath);

            Console.WriteLine($"Word count saved in: {outputPath}");

            spark.Stop();
        }
    }
}

//spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master spark://spark-master:7077 --conf spark.dotnet.ignoreSparkPatchVersionCheck=true /opt/spark-apps/Dotnet/WordCountSpark/publish/Microsoft.Spark.dll /opt/spark-apps/Dotnet/WordCountSpark/publish/WordCountSpark.dll /opt/spark-data/random_text.txt
