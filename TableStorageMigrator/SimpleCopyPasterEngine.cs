using System;
using System.Threading.Tasks;

namespace TableStorageMigrator
{

    public static class SimpleCopyPasteEngine
    {

        public static async Task RunSimpleCopyPasteAsync(this SettingsModel settings)
        {
            
            Console.WriteLine("Simple copy/past mode....");

            foreach (var srcTable in settings.GetSrcTables())
            {
                Console.WriteLine("Copying table: " + srcTable.CloudTable);

                var destTable = settings.DestConnString.GetAzureTable(srcTable.TableName);

                var copyPasteEngine = new CopyPasteEngine(srcTable, destTable);

                await copyPasteEngine.TheTask;

                Console.WriteLine("Copied table: " + srcTable.TableName);

                Console.WriteLine("Matching entities: " + srcTable.TableName);

                if (settings.Verify)
                   await srcTable.VerifyAsync(destTable);

            }
        }


    }

}