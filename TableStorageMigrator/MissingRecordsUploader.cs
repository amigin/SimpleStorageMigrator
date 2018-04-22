
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
                Console.WriteLine("Loading table: " + srcTable.CloudTable);

                var buffer = new Dictionary<string, Dictionary<string, DynamicTableEntity>>();

                var srcLoadedCount = 0;

                await srcTable.GetEntitiesByChunkAsync(chunk =>
                {

                    foreach (var entity in chunk)
                    {
                        if (!buffer.ContainsKey(entity.PartitionKey))
                            buffer.Add(entity.PartitionKey, new Dictionary<string, DynamicTableEntity>());
                        buffer[entity.PartitionKey].Add(entity.RowKey, entity);

                        Console.CursorLeft = 0;
                    }
                    
                    srcLoadedCount += chunk.Length;

                    Console.Write("Loaded entities: " + srcLoadedCount);

                    return Task.FromResult(0);

                });
                Console.WriteLine("");
                Console.WriteLine("Loaded Source Table"+srcTable.CloudTable.Name);

                var destTable = settings.DestConnString.GetAzureTable(srcTable.TableName);

                var loadedDest = 0;
                var removedDest = 0;
                await destTable.GetEntitiesByChunkAsync(chunk =>
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

                    Console.Write($"Loaded {loadedDest} at dest table. Removed: " + removedDest+"        ");
                    Console.CursorLeft = 0;                    

                    return Task.FromResult(0);

                });

                Console.WriteLine("");
                Console.WriteLine("Loaded Dest Table: "+destTable.CloudTable.Name);


                var inserted = 0;
                
                
                if (buffer.Count ==0)
                    Console.WriteLine("Nothing to sync for table: "+destTable.CloudTable.Name);
                

                foreach (var kvp in buffer)
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

    }

}
