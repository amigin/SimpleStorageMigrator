
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using MoreLinq;

namespace TableStorageMigrator
{

    public static class MissingRecordsUploader
    {

        public static async Task UploadMissingRecordsAsync(this SettingsModel settings)
        {

            
            Console.WriteLine("Uploading missing records mode....");
            
            
            foreach (var srcTable in settings.GetSrcTables())
            {
                Console.WriteLine("");
                Console.WriteLine("Loading table: " + srcTable.CloudTable);


                var srcLoadedCount = 0;

                var destLoadedCount = 0;



                var srcLoadBufferTask = srcTable.LoadDataToCache(c =>
                {                    
                    srcLoadedCount = c;
                    Console.Write($"Src Loaded: {srcLoadedCount}; Dest Loaded: {destLoadedCount}");
                    Console.CursorLeft = 0;  
                });
                
                var destTable = settings.DestConnString.GetAzureTable(srcTable.TableName);
                var destLoadBufferTask = destTable.LoadDataToCache(c =>
                {
                    
                    destLoadedCount = c;
                    Console.Write($"Src Loaded: {srcLoadedCount}; Dest Loaded: {destLoadedCount}"); 
                    Console.CursorLeft = 0;  
                });

                Console.WriteLine();

                var srcBuffer = await srcLoadBufferTask;
                var destBuffer = await destLoadBufferTask;


                CleanEntitiesFromSource(srcBuffer, destBuffer);
                
                var inserted = 0;
                
                if (srcBuffer.Count ==0)
                    Console.WriteLine("Nothing to sync for table: "+destTable.CloudTable.Name);
                

                foreach (var kvp in srcBuffer)
                {
                    Console.WriteLine("");
                    Console.WriteLine("");
                    
                    Console.WriteLine("Syncinc Partition: " + kvp.Key);

                    foreach (var chunk in kvp.Value.Values.Batch(1000))
                    {
                        var chunkToUpload = chunk.ToArray();
                        await destTable.InsertAsync(chunkToUpload);
                        inserted += chunkToUpload.Length;
                        Console.Write("Inserted missing records: " + inserted);
                        Console.CursorLeft = 0;                        
                    }

                }
            }
        }


        private static async Task<Dictionary<string, Dictionary<string, DynamicTableEntity>>> LoadDataToCache(
            this TableEntitySdk table, Action<int> loadedCallback)
        {
            var buffer = new Dictionary<string, Dictionary<string, DynamicTableEntity>>();

            var loaded = 0;

            await table.GetEntitiesByChunkAsync(chunk =>
            {

                foreach (var entity in chunk)
                {
                    if (!buffer.ContainsKey(entity.PartitionKey))
                        buffer.Add(entity.PartitionKey, new Dictionary<string, DynamicTableEntity>());
                    buffer[entity.PartitionKey].Add(entity.RowKey, entity);

                    Console.CursorLeft = 0;
                }

                loaded += chunk.Length;

                loadedCallback(loaded);

                return Task.FromResult(0);

            });

            return buffer;

        }


        private static void CleanEntitiesFromSource(Dictionary<string, Dictionary<string, DynamicTableEntity>> src, Dictionary<string, Dictionary<string, DynamicTableEntity>> dest)
        {

            foreach (var destRow in dest)
            foreach (var destEntity in destRow.Value.Values)
            {
                if (!src.ContainsKey(destEntity.PartitionKey)) continue;

                if (src[destEntity.PartitionKey].ContainsKey(destEntity.RowKey))
                    src[destEntity.PartitionKey].Remove(destEntity.RowKey);

                if (src[destEntity.PartitionKey].Count == 0)
                    src.Remove(destEntity.PartitionKey);

            }

        }
        
    }

}
