using System;

namespace TableStorageMigrator
{

    public static class SimpleCopyPasteEngine
    {

        public static void CopyPaste(string srcConnString, string destConnString, bool verifyTables)
        {

            foreach (var srcTable in SettingsReader.GetSrcTables(srcConnString))
            {
                Console.WriteLine("Copying table: " + srcTable.CloudTable);

                var destTable = destConnString.GetAzureTable(srcTable.TableName);

                var copyPasteEngine = new CopyPasteEngine(srcTable, destTable);

                copyPasteEngine.TheTask.Wait();

                Console.WriteLine("Copied table: " + srcTable.TableName);

                Console.WriteLine("Matching entities: " + srcTable.TableName);

                if (verifyTables)
                    srcTable.Verify(destTable);

            }
        }


    }

}