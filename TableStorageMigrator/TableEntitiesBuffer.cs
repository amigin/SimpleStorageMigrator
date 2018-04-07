using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageMigrator
{


    public class TableStoragePartition
    {
        private readonly Dictionary<string, DynamicTableEntity> _entities = new Dictionary<string, DynamicTableEntity>();


        public void Add(DynamicTableEntity entity)
        {
            _entities.Add(entity.RowKey, entity);
        }

        public bool HasEntity(DynamicTableEntity entity)
        {
            return _entities.ContainsKey(entity.RowKey);
        }
    }
    

    public class TableEntitiesBuffer
    {
        
        private readonly Dictionary<string, TableStoragePartition> _partitions = new Dictionary<string, TableStoragePartition>();

        public void Add(DynamicTableEntity entity)
        {
            if (!_partitions.ContainsKey(entity.PartitionKey))
                _partitions.Add(entity.PartitionKey, new TableStoragePartition());
            
            _partitions[entity.PartitionKey].Add(entity);
        }

        public bool HasEntity(DynamicTableEntity entity)
        {

            if (!_partitions.ContainsKey(entity.PartitionKey))
                return false;

            return _partitions[entity.PartitionKey].HasEntity(entity);
        }

        public void Add(IEnumerable<DynamicTableEntity> entities)
        {
            foreach (var entity in entities)
                Add(entity);
        }

    }


}