using System;

namespace TableStorageMigrator
{

    public static class SimpleCopyPasteEngine
    {

        public static void RunSimpleCopyPaste(this SettingsModel settings)
        {
            
            Console.WriteLine("Simple copy/past mode....");

            foreach (var srcTable in settings.GetSrcTables())
            {
                Console.WriteLine("Copying table: " + srcTable.CloudTable);

                var destTable = settings.DestConnString.GetAzureTable(srcTable.TableName);

                var copyPasteEngine = new CopyPasteEngine(srcTable, destTable);

                copyPasteEngine.TheTask.Wait();

                Console.WriteLine("Copied table: " + srcTable.TableName);

                Console.WriteLine("Matching entities: " + srcTable.TableName);

                if (settings.Verify)
                    srcTable.Verify(destTable);

            }
        }


    }

}