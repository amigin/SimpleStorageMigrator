
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

                srcTable.GetEntitiesByChunkAsync(chunk =>
                {

                    foreach (var entity in chunk)
                    {
                        if (!buffer.ContainsKey(entity.PartitionKey))
                            buffer.Add(entity.PartitionKey, new Dictionary<string, DynamicTableEntity>());
                        buffer[entity.PartitionKey].Add(entity.RowKey, entity);

                        Console.WriteLine("Loaded entities: " + chunk.Length);
                    }

                    return Task.FromResult(0);

                }).Wait();

                var destTable = settings.DestConnString.GetAzureTable(srcTable.TableName);

                destTable.GetEntitiesByChunkAsync(chunk =>
                {

                    var removed = 0;

                    foreach (var entity in chunk)
                    {
                        if (!buffer.ContainsKey(entity.PartitionKey))
                            continue;

                        var partition = buffer[entity.PartitionKey];

                        if (partition.ContainsKey(entity.RowKey))
                            partition.Remove(entity.RowKey);

                        if (partition.Count == 0)
                            buffer.Remove(entity.PartitionKey);

                        removed--;

                    }

                    Console.WriteLine($"Loaded {chunk.Length} at dest table. Removed: " + removed);

                    return Task.FromResult(0);

                }).Wait();


                foreach (var kvp in buffer)
                {
                    Console.WriteLine("Syncinc Partition: " + kvp.Key);

                    foreach (var chunk in kvp.Value.Values.Batch(1000))
                    {
                        var chunkToUpload = chunk.ToArray();
                        destTable.InsertAsync(chunkToUpload).Wait();
                        Console.WriteLine("Inserted missing records: " + chunkToUpload.Length);
                    }

                }
            }
        }

    }

}
