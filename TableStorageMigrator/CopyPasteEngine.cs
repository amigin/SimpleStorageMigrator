using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageMigrator
{
    public class CopyPasteEngine
    {
        private readonly TableEntitySdk _srcTable;
        private readonly TableEntitySdk _destTable;
        private readonly Queue<DynamicTableEntity[]> _queue = new Queue<DynamicTableEntity[]>();

        public Task ReadTask { get; }
        public Task WriteTask { get; }
        

        public CopyPasteEngine(TableEntitySdk srcTable, TableEntitySdk destTable)
        {
            _srcTable = srcTable;
            _destTable = destTable;

            ReadTask = ReadTaskAsync();
            WriteTask = WriteTaskAsync();

        }

        private async Task ReadTaskAsync()
        {
            var read = 0;
            await _srcTable.GetEntitiesByChunkAsync(async chunk =>
            {

                await _queue.WriteAsync(chunk);
                read += chunk.Length;
                Console.WriteLine("Read entities : " + read);

            });
            _queue.WriteEof();

        }


        private async Task WriteTaskAsync()
        {

            var written = 0;
            while (true)
            {
                var entities = await _queue.ReadAsync();

                if (entities == null)
                    break;

                await _destTable.InsertAsync(entities);
                written += entities.Length;
                Console.WriteLine("Written entities : " + written);
            }

        }

    }


    public static class CopyPasteExtentions
    {

        public static void WriteEof(this Queue<DynamicTableEntity[]> queue)
        {
            lock (queue)
            {
                queue.Enqueue(null);
            }
        }

        public static async Task WriteAsync(this Queue<DynamicTableEntity[]> queue, DynamicTableEntity[] entities)
        {
            while (true)
            {

                lock (queue)
                {
                    if (queue.Count < 5)
                    {
                        queue.Enqueue(entities);
                        return;
                    }
                }

                await Task.Delay(500);

            }

        }

        public static async Task<DynamicTableEntity[]> ReadAsync(this Queue<DynamicTableEntity[]> queue)
        {

            while (true)
            {
                lock (queue)
                {
                    if (queue.Count > 0)
                        return queue.Dequeue();
                }

                await Task.Delay(100);
            }
        }
    }
}