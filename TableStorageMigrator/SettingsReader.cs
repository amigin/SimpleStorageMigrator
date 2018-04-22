using System;
using System.Collections.Generic;
using System.IO;

namespace TableStorageMigrator
{


    public class SettingsModel
    {

        public string SrcConnString { get; set; }

        public string DestConnString { get; set; }


        //Null - all tables
        public string[] TablesToCopy { get; set; }


        public const string SimpleCopyMode = "Copy";
        public const string UploadNonExistMode = "UpoloadNonExist";

        public string Mode { get; set; }
        
        public bool Verify { get; set; }


    }


    public static class SettingsReader
    {


        public static SettingsModel GetSettings()
        {
            var settingsFileName = Environment.GetEnvironmentVariable("TableStorageMigratorSettingsFile");

            if (string.IsNullOrEmpty(settingsFileName))
                settingsFileName = Environment.CurrentDirectory + "/settings.json";


            if (!File.Exists(settingsFileName))
                throw new Exception(settingsFileName + " does not exist");


            var json = File.ReadAllText(settingsFileName);

            return Newtonsoft.Json.JsonConvert.DeserializeObject<SettingsModel>(json);

        }



        public static IEnumerable<TableEntitySdk> GetSrcTables(this SettingsModel settings)
        {


            if (settings.TablesToCopy == null)
            {
                Console.WriteLine("Copying all tables");
                return settings.SrcConnString.GetTables();
            }


            var result = new List<TableEntitySdk>();

            foreach (var tableName in settings.TablesToCopy)
            {
                result.Add(settings.SrcConnString.GetAzureTable(tableName));
            }

            return result;

        }

    }

}
