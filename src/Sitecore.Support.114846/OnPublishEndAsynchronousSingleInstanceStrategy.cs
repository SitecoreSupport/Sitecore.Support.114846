namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using System.Collections.Generic;
    using System.Linq;
    using Configuration;
    using Data.Eventing;
    using Eventing;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.Data.Archiving;
    using Sitecore.Data.Eventing.Remote;

    public class OnPublishEndAsynchronousSingleInstanceStrategy :
        Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousSingleInstanceStrategy
    {
        public OnPublishEndAsynchronousSingleInstanceStrategy(string database) : base(database)
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

            var lastEventQueueStamp = this.GetLastProcessedEventTimestamp(eventQueue);
            query.ToTimestamp = lastEventQueueStamp;

            query.EventTypes.Add(typeof(RemovedVersionRemoteEvent));
            query.EventTypes.Add(typeof(SavedItemRemoteEvent));
            query.EventTypes.Add(typeof(DeletedItemRemoteEvent));
            query.EventTypes.Add(typeof(MovedItemRemoteEvent));
            query.EventTypes.Add(typeof(AddedVersionRemoteEvent));
            query.EventTypes.Add(typeof(CopiedItemRemoteEvent));
            query.EventTypes.Add(typeof(RestoreItemCompletedEvent));
            list.AddRange(eventQueue.GetQueuedEvents(query));

            var eventRecords = list.Where(e => e.Timestamp > lastUpdatedTimestamp).ToList();

            CrawlingLog.Log.Info(string.Format("SUPPORT [Index={0}] OnPublishEndAsynchronousSingleInstanceStrategy: Processing queue [{1}; {2}] Count: {3}", this.Index.Name, lastUpdatedTimestamp, lastEventQueueStamp, eventRecords.Count));

            return eventRecords;
        }

        protected virtual long? GetLastProcessedEventTimestamp(EventQueue eventQueue)
        {
            var eqEx = eventQueue as SqlServerEventQueue;
            return eqEx?.GetLastProcessedStamp;
        }
    }
}