
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

        public static void UploadMissingRecords(this SettingsModel settings)
        {

            
            Console.WriteLine("Uploading missing records mode....");
            
            
            foreach (var srcTable in settings.GetSrcTables())
            {
                Console.WriteLine("Loading table: " + srcTable.CloudTable);

                var buffer = new Dictionary<string, Dictionary<string, DynamicTableEntity>>();

                var srcLoadedCount = 0;

                srcTable.GetEntitiesByChunkAsync(chunk =>
                {

                    foreach (var entity in chunk)
                    {
                        if (!buffer.ContainsKey(entity.PartitionKey))
                            buffer.Add(entity.PartitionKey, new Dictionary<string, DynamicTableEntity>());
                        buffer[entity.PartitionKey].Add(entity.RowKey, entity);

                        srcLoadedCount += chunk.Length;

                        Console.WriteLine("Loaded entities: " + srcLoadedCount);

                        Console.CursorLeft = 0;
                    }

                    return Task.FromResult(0);

                }).Wait();
                
                Console.WriteLine("");
                Console.WriteLine("");

                var destTable = settings.DestConnString.GetAzureTable(srcTable.TableName);

                var loadedDest = 0;
                var removedDest = 0;
                destTable.GetEntitiesByChunkAsync(chunk =>
                {

                    foreach (var entity in chunk)
                    {
                        if (!buffer.ContainsKey(entity.PartitionKey))
                            continue;

                        var partition = buffer[entity.PartitionKey];

                        if (partition.ContainsKey(entity.RowKey))
                            partition.Remove(entity.RowKey);

                        if (partition.Count == 0)
                            buffer.Remove(entity.PartitionKey);

                        removedDest++;

                    }

                    loadedDest += chunk.Length;

                    Console.WriteLine($"Loaded {loadedDest} at dest table. Removed: " + removedDest+"        ");
                    Console.CursorLeft = 0;                    

                    return Task.FromResult(0);

                }).Wait();



                var inserted = 0;
                

                foreach (var kvp in buffer)
                {
                    Console.WriteLine("Syncinc Partition: " + kvp.Key);

                    foreach (var chunk in kvp.Value.Values.Batch(1000))
                    {
                        var chunkToUpload = chunk.ToArray();
                        destTable.InsertAsync(chunkToUpload).Wait();
                        inserted += chunkToUpload.Length;
                        Console.WriteLine("Inserted missing records: " + inserted);
                    }

                }
            }
        }

    }

}
