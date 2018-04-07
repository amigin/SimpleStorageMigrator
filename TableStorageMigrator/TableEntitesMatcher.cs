using System;
using System.Threading.Tasks;
using Common;

namespace TableStorageMigrator
{

    public static class TableEntitesMatcher
    {
        public static async Task MatchEntitiesAsync(this TableEntitiesBuffer buffer, TableEntitySdk tableEntitySdk)
        {
            var matched = 0;

            await tableEntitySdk.GetEntitiesByChunkAsync(chunk =>
            {
                return Task.Run(() =>
                {
                    foreach (var entity in chunk)
                    {
                        if (!buffer.HasEntity(entity))
                            throw new Exception("Partition " + entity.ToJson() +
                                                " is not found at the dest tablestorage");
                    }

                    matched += chunk.Length;

                    Console.WriteLine("Matched entities: " + matched);
                });
            });

        }

    }
    
    
}