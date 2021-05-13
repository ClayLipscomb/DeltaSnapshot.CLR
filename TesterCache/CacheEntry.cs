using System;
using System.Text;
using DeltaSnapshot;

namespace TesterCache {
    public class CacheEntry<TEntity> where TEntity : class, IDataSetEntity, new() {
        public long CacheSnapshotId { get; set; }
        public int SubscriptionDataSetId { get; set; }
        public long RunId { get; set; }
        public string EntityIdentifier { get; set; }
        public string EntityDeltaCode { get; set; }
        public DateTimeOffset EntityDeltaDate { get; set; }
        public TEntity EntityDataCurrent { get; set; }
        public TEntity EntityDataPrevious { get; set; }
    }
}