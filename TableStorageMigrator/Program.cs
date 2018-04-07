using System;

namespace TableStorageMigrator
{
    class Program
    {
        static void Main(string[] args)
        {
            var srcConnString = Environment.GetEnvironmentVariable("SrcConnString");
            if (string.IsNullOrEmpty(srcConnString))
                throw new Exception("Please specift 'SrcConnString' env variable");

            var destConnString = Environment.GetEnvironmentVariable("DestConnString");
            if (string.IsNullOrEmpty(destConnString))
                throw new Exception("Please specift 'DestConnString' env variable");

            var tablesFromEnvVariable = Environment.GetEnvironmentVariable("CopyTable");
            if (string.IsNullOrEmpty(tablesFromEnvVariable))
                throw new Exception("Please specift 'DestConnString' env variable");


            var tables = tablesFromEnvVariable.Split('|');


            foreach (var tableToCopy in tables)
            {
                Console.WriteLine("Copying table: " + tableToCopy);


                var srcTable = srcConnString.GetAzureTable(tableToCopy);
                var destTable = destConnString.GetAzureTable(tableToCopy);

                var copyPasteEngine = new CopyPasteEngine(srcTable, destTable);

                copyPasteEngine.TheTask.Wait();
                
                Console.WriteLine("Copied table: " + tableToCopy);    
                
                Console.WriteLine("Matching entities: " + tableToCopy);

                //copyPasteEngine.EntitiesBuffer.MatchEntitiesAsync(destTable).Wait();
                var matched = srcTable.EqualTo(destTable).Result;

                if (matched)
                    Console.WriteLine("Done with table: " + tableToCopy);
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Table data does not match!");
                    Console.ForegroundColor = ConsoleColor.Gray;
                }

            }

            Console.WriteLine("Done....");
            Console.ReadLine();
        }
    }
}