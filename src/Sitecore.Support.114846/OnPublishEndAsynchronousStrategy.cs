
namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using Configuration;
    using Data.Archiving;
    using Data.Eventing.Remote;
    using Eventing;
    using Sitecore.ContentSearch.Diagnostics;

    [DataContract]
    public class OnPublishEndAsynchronousStrategy :
        Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy
    {
        public OnPublishEndAsynchronousStrategy(string database) : base(database)
        {
        }

        protected override List<QueuedEvent> ReadQueue(EventQueue eventQueue, long? lastUpdatedTimestamp)
        {
            var list = new List<QueuedEvent>();
            var nullable = lastUpdatedTimestamp;
            lastUpdatedTimestamp = nullable.HasValue ? nullable.GetValueOrDefault() : 0L;
            var query = new EventQueueQuery
            {
                FromTimestamp = lastUpdatedTimestamp
            };

            var lastEventQueueStamp = GetLastProcessedEventTimestamp(eventQueue);
            query.ToTimestamp = lastEventQueueStamp;

            query.EventTypes.Add(typeof(RemovedVersionRemoteEvent));
            query.EventTypes.Add(typeof(SavedItemRemoteEvent));
            query.EventTypes.Add(typeof(DeletedItemRemoteEvent));
            query.EventTypes.Add(typeof(MovedItemRemoteEvent));
            query.EventTypes.Add(typeof(AddedVersionRemoteEvent));
            query.EventTypes.Add(typeof(CopiedItemRemoteEvent));
            query.EventTypes.Add(typeof(RestoreItemCompletedEvent));
            list.AddRange(eventQueue.GetQueuedEvents(query));
            return (from e in list
                where e.Timestamp > lastUpdatedTimestamp
                select e).ToList();
        }

        protected virtual long? GetLastProcessedEventTimestamp(EventQueue eventQueue)
        {
            var lastProcessedStamp = Database.Properties["EQStamp_" + Settings.InstanceName];

            if (string.IsNullOrEmpty(lastProcessedStamp))
            {
                return null;
            }

            long num;
            if (long.TryParse(lastProcessedStamp, out num))
            {
                return num;
            }

            CrawlingLog.Log.Warn($"SUPPORT Can't parse the stamp '{lastProcessedStamp}'");

            return null;
        }
    }
}