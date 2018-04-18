using System;
using System.Collections.Generic;

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



            var matchDataVal = Environment.GetEnvironmentVariable("MatchData");

            var matchData = true;
            if (!string.IsNullOrEmpty(matchDataVal))
            {
                var parsed = bool.TryParse(matchDataVal, out matchData);
                if (!parsed)
                    throw new Exception("Invalid value for 'MatchData' var. Please set 'true' or 'false'");
            }






            foreach (var srcTable in GetSrcTables(srcConnString))
            {
                Console.WriteLine("Copying table: " + srcTable.CloudTable);


                var destTable = destConnString.GetAzureTable(srcTable.TableName);

                var copyPasteEngine = new CopyPasteEngine(srcTable, destTable);

                copyPasteEngine.TheTask.Wait();

                Console.WriteLine("Copied table: " + srcTable.TableName);

                Console.WriteLine("Matching entities: " + srcTable.TableName);

                if (matchData)
                {
                    //copyPasteEngine.EntitiesBuffer.MatchEntitiesAsync(destTable).Wait();
                    var error = srcTable.EqualTo(destTable, i =>
                    {
                        if (i % 1000 == 0)
                        {
                            Console.WriteLine($"   {i} items matched...");
                        }
                    }).Result;

                    if (error != null)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Table data does not match!");
                        Console.WriteLine(error.Msg);
                        Console.ForegroundColor = ConsoleColor.Gray;
                    }
                    else
                        Console.WriteLine("Done with table: " + srcTable.TableName);
                }
            }

            Console.WriteLine("Done....");
            Console.ReadLine();
        }

        private static IEnumerable<TableEntitySdk> GetSrcTables(string srcConnString)
        {
            var tablesFromEnvVariable = Environment.GetEnvironmentVariable("CopyTable");
            if (string.IsNullOrEmpty(tablesFromEnvVariable))
                throw new Exception("Please specift 'CopyTable' env variable");

            if (tablesFromEnvVariable == "*")
                return TableStorageSdk.GetTables(srcConnString);


            var result = new List<TableEntitySdk>();

            foreach (var tableName in tablesFromEnvVariable.Split('|'))
            {
                result.Add(TableStorageSdk.GetAzureTable(srcConnString, tableName));
            }

            return result;

        }

    }



}