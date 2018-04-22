using System;
using System.Threading.Tasks;

namespace TableStorageMigrator
{

    public static class VerificationEngine
    {

        public static async Task VerifyAsync(this TableEntitySdk srcTable, TableEntitySdk destTable)
        {

            //copyPasteEngine.EntitiesBuffer.MatchEntitiesAsync(destTable).Wait();
            var error = await srcTable.EqualToAsync(destTable, i =>
            {
                if (i % 1000 == 0)
                {
                    Console.WriteLine($"   {i} items matched...");
                }
            });

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

}