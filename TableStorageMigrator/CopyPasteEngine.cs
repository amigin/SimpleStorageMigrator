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

        public Task TheTask { get; }

        
        public readonly TableEntitiesBuffer EntitiesBuffer = new TableEntitiesBuffer();

        public CopyPasteEngine(TableEntitySdk srcTable, TableEntitySdk destTable)
        {
            _srcTable = srcTable;
            _destTable = destTable;

            TheTask = ReadWriteTaskAsync();
        }

        private async Task ReadWriteTaskAsync()
        {
            var written = 0;
            await _srcTable.GetEntitiesByChunkAsync(async chunk =>
            {

                EntitiesBuffer.Add(chunk);
                await _destTable.InsertAsync(chunk);
                written += chunk.Length;
                Console.WriteLine("Written entities : " + written);

            });
        }

    }
}