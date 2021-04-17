using System;
using System.Text;
using DeltaSnapshot;

namespace TesterCs {
    public class CacheEntry<T> : ICacheEntryType<T> where T : class, IDataSetEntity, new() {
        public long CacheEntryId { get; set; }
        public int DataSetId { get; set; }
        public long RunId { get; set; }
        public string EntityIdentifier { get; set; }
        public string EntityDeltaCode { get; set; }
        public DateTimeOffset EntityDeltaDate { get; set; }
        public T EntityDataCurrent { get; set; }
        public T EntityDataPrevious { get; set; }
    }
}