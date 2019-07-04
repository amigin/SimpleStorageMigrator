using System;
using System.Threading.Tasks;

namespace TableStorageMigrator
{
    public static class SimpleCopyPasteEngine
    {
        public static async Task RunSimpleCopyPasteAsync(this SettingsModel settings)
        {
            Console.WriteLine("Simple copy/past mode....");
            var date = DateTime.UtcNow;

            foreach (var srcTable in settings.GetSrcTables())
            {
                string destTableName = settings.AddDateToDestTableName
                    ? $"{srcTable.TableName}{date:yyyyMMddHHmm}"
                    : srcTable.TableName;

                Console.WriteLine($"Copying table: {srcTable.CloudTable}, destination table: {destTableName}");

                var destTable = settings.DestConnString.GetAzureTable(destTableName);

                var copyPasteEngine = new CopyPasteEngine(srcTable, destTable, settings.SkipBuffer);

                await copyPasteEngine.TheTask;

                Console.WriteLine("Copied table: " + srcTable.TableName);

                Console.WriteLine("Matching entities: " + srcTable.TableName);

                if (settings.Verify)
                   await srcTable.VerifyAsync(destTable);
            }
        }
    }
}
