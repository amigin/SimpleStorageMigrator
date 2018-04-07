using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using MoreLinq;

namespace TableStorageMigrator
{
    public static class TableEntitySdkComparator
    {
        public static async Task<bool> EqualTo(this TableEntitySdk src, TableEntitySdk dest)
        {
            var rangeQuery = new TableQuery<DynamicTableEntity>();

            TableContinuationToken srcTableContinuationToken = null;
            TableContinuationToken destTableContinuationToken = null;

            do
            {
                var srcQuery = src.CloudTable.ExecuteQuerySegmentedAsync(rangeQuery, srcTableContinuationToken);
                var destQuery = dest.CloudTable.ExecuteQuerySegmentedAsync(rangeQuery, destTableContinuationToken);

                var srcQueryResponse = await srcQuery;
                srcTableContinuationToken = srcQueryResponse.ContinuationToken;
                var srcResult = srcQueryResponse.Results.ToArray();

                var destQueryResponse = await destQuery;
                destTableContinuationToken = destQueryResponse.ContinuationToken;
                var destResult = destQueryResponse.Results.ToArray();

                if (srcResult.Length != destResult.Length)
                    return false;

                for (int i = 0; i < srcResult.Length; i++)
                {
                    if (!srcResult[i].EqualTo(destResult[i]))
                        return false;
                }

            } while (srcTableContinuationToken != null);

            //records left in dest storage
            if (destTableContinuationToken != null)
                return false;

            return true;
        }
    }

    public class TableEntitySdk
    {
        public CloudStorageAccount CloudStorageAccount { get; private set; }
        public CloudTable CloudTable { get; private set; }


        public int Count { get; private set; }

        public int Errors { get; set; }

        public TableEntitySdk(CloudStorageAccount cloudStorageAccount, CloudTable cloudTable)
        {
            CloudStorageAccount = cloudStorageAccount;
            CloudTable = cloudTable;
        }

        public TableEntitySdk(string connString, string tableName)
        {
            CloudStorageAccount = CloudStorageAccount.Parse(connString);
            var tableClient = CloudStorageAccount.CreateCloudTableClient();

            CloudTable = tableClient.GetTableReference(tableName);

            CloudTable.CreateIfNotExistsAsync().Wait();
        }

        public async Task GetEntitiesByChunkAsync(Func<DynamicTableEntity[], Task> chunkCallback)
        {

            var rangeQuery = new TableQuery<DynamicTableEntity>();

            TableContinuationToken tableContinuationToken = null;
            do
            {
                var queryResponse = await CloudTable.ExecuteQuerySegmentedAsync(rangeQuery, tableContinuationToken);
                tableContinuationToken = queryResponse.ContinuationToken;
                var result = queryResponse.Results.ToArray();
                Count += result.Length;
                await chunkCallback(result);
            } while (tableContinuationToken != null);

        }

        public async Task InsertAsync(DynamicTableEntity[] entities)
        {

            var tasks = new List<Task<IList<TableResult>>>();

            foreach (var group in entities.GroupBy(e => e.PartitionKey))
            {
                foreach (var batch in group.Batch(100))
                {
                    var batchCommand = new TableBatchOperation();

                    foreach (var entity in batch)
                        batchCommand.Insert(entity);

                    tasks.Add(CloudTable.ExecuteBatchAsync(batchCommand));
                }
            }

            foreach (var task in tasks)
            {

                try
                {

                    var results = await task;

                    foreach (var result in results)
                    {

                        if (result.HttpStatusCode >= 300)
                            Console.WriteLine("Something wrong with entity: " + result.Result.ToJson());

                    }

                }
                catch (Exception)
                {
                    Errors++;
                }

            }

            Count += entities.Length;
        }

    }



    public static class TableStorageSdk
    {
        public static IEnumerable<CloudTable> GetTables(this CloudStorageAccount cloudStorageAccount)
        {
            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();

            TableContinuationToken continuationToken = null;
            do
            {
                var segment = cloudTableClient.ListTablesSegmentedAsync(continuationToken).Result;
                continuationToken = segment.ContinuationToken;

                foreach (var table in segment.Results)
                    yield return table;

            } while (continuationToken != null);
        }


        public static IEnumerable<TableEntitySdk> GetTables(this string connString)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connString);

            foreach (var tableName in cloudStorageAccount.GetTables())
            {
                yield return new TableEntitySdk(cloudStorageAccount, tableName);
            }
        }
        
        public static TableEntitySdk GetAzureTable(this string connString, string tableName)
        {
            return new TableEntitySdk(connString, tableName);
        }

        public static bool EqualTo(this DynamicTableEntity src, DynamicTableEntity dest)
        {
            return src.GetHashCodeExt() == dest.GetHashCodeExt();
        }

        public static int GetHashCodeExt(this DynamicTableEntity entity)
        {
            StringBuilder sb = new StringBuilder();

            int hashCode = (entity.PartitionKey, entity.RowKey).GetHashCode();

            foreach (var prop in entity.Properties)
            {
                hashCode = hashCode * 31 + prop.Value.PropertyAsObject.GetHashCode();
            }

            return hashCode;
        }
    }
}