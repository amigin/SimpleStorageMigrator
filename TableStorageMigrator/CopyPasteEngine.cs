using System;
using System.Threading.Tasks;

namespace TableStorageMigrator
{
    public class CopyPasteEngine
    {
        private readonly TableEntitySdk _srcTable;
        private readonly TableEntitySdk _destTable;
        private readonly bool _skipBuffer;

        public Task TheTask { get; }

        public readonly TableEntitiesBuffer EntitiesBuffer = new TableEntitiesBuffer();

        public CopyPasteEngine(TableEntitySdk srcTable, TableEntitySdk destTable, bool skipBuffer)
        {
            _srcTable = srcTable;
            _destTable = destTable;
            _skipBuffer = skipBuffer;

            TheTask = ReadWriteTaskAsync();
        }

        private async Task ReadWriteTaskAsync()
        {
            var written = 0;
            await _srcTable.GetEntitiesByChunkAsync(async chunk =>
            {
                if (!_skipBuffer)
                    EntitiesBuffer.Add(chunk);

                await _destTable.InsertAsync(chunk);
                written += chunk.Length;
                Console.WriteLine("Written entities : " + written);
            });
        }
    }
}
