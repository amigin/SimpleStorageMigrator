using System;
using System.Collections.Generic;

namespace TableStorageMigrator
{

    public static class SettingsReader
    {
        public static IEnumerable<TableEntitySdk> GetSrcTables(string srcConnString)
        {
            var tablesFromEnvVariable = Environment.GetEnvironmentVariable("CopyTable");
            if (string.IsNullOrEmpty(tablesFromEnvVariable))
                throw new Exception("Please specift 'CopyTable' env variable");

            if (tablesFromEnvVariable.Contains("*"))
            {
                Console.WriteLine("Copying all tables");
                return TableStorageSdk.GetTables(srcConnString);
            }


            var result = new List<TableEntitySdk>();

            foreach (var tableName in tablesFromEnvVariable.Split('|'))
            {
                result.Add(TableStorageSdk.GetAzureTable(srcConnString, tableName));
            }

            return result;

        }


        public static bool VerifyTables()
        {
            var matchDataVal = Environment.GetEnvironmentVariable("MatchData");

            if (string.IsNullOrEmpty(matchDataVal))
                return true;


            var parsed = bool.TryParse(matchDataVal, out var matchData);
            if (!parsed)
                return true;

            return matchData;
        }
    }

}
