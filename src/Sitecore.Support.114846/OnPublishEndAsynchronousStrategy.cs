
namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using Data.Eventing;
    using Sitecore.Common;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.ContentSearch.Maintenance.Strategies;
    using Sitecore.ContentSearch.Utilities;
    using Sitecore.Data;
    using Sitecore.Data.Eventing.Remote;
    using Sitecore.Diagnostics;
    using Sitecore.Eventing;
    using Sitecore.Eventing.Remote;
    using Sitecore.Globalization;

    [DataContract]
    public class OnPublishEndAsynchronousStrategy : IIndexUpdateStrategy
    {
        private ISearchIndex index;

        private IContentSearchConfigurationSettings contentSearchSettings;

        public OnPublishEndAsynchronousStrategy(string database)
        {
            Assert.IsNotNullOrEmpty(database, "database");
            this.Database = Database.GetDatabase(database);
            Assert.IsNotNull(this.Database, string.Format("Database '{0}' was not found", database));
        }

        public bool CheckForThreshold { get; set; }

        public Database Database { get; protected set; }

        public void Initialize(ISearchIndex searchIndex)
        {
            Assert.IsNotNull(searchIndex, "index");
            CrawlingLog.Log.Info(string.Format("[Index={0}] Initializing OnPublishEndAsynchronousStrategy.", searchIndex.Name));

            this.index = searchIndex;

            this.contentSearchSettings = this.index.Locator.GetInstance<IContentSearchConfigurationSettings>();

            if (!Sitecore.Configuration.Settings.GetBoolSetting("EventQueue.Enabled", true))
            {
                CrawlingLog.Log.Fatal(string.Format("[Index={0}] Initialization of OnPublishEndAsynchronousStrategy failed because event queue is not enabled.", searchIndex.Name));
                return;
            }

            EventHub.PublishEnd += (sender, args) => this.Handle();
        }

        public void Run()
        {
            CrawlingLog.Log.Debug(string.Format("[Index={0}] OnPublishEndAsynchronousStrategy triggered.", this.index.Name));

            if (this.Database == null)
            {
                CrawlingLog.Log.Fatal(string.Format("[Index={0}] OperationMonitor has invalid parameters. Index Update cancelled.", this.index.Name));
                return;
            }

            var eventQueue = this.Database.RemoteEvents.Queue;

            if (eventQueue == null)
            {
                CrawlingLog.Log.Fatal(string.Format("[Index={0}] Event Queue is empty. Returning.", this.index.Name));
                return;
            }

            EventManager.RaiseQueuedEvents(); 

            var queue = this.ReadQueue(eventQueue);

            if (queue.Count <= 0)
            {
                CrawlingLog.Log.Debug(string.Format("[Index={0}] Event Queue is empty. Incremental update returns", this.index.Name));
                return;
            }

            if (this.CheckForThreshold && queue.Count > this.contentSearchSettings.FullRebuildItemCountThreshold())
            {
                CrawlingLog.Log.Warn(string.Format("[Index={0}] The number of changes exceeded maximum threshold of '{1}'.", this.index.Name, this.contentSearchSettings.FullRebuildItemCountThreshold()));
                IndexCustodian.FullRebuild(this.index).Wait();
                return;
            }

            var indexableInfos = this.ExtractIndexableInfoFromQueue(queue);
           
            IndexCustodian.IncrementalUpdate(this.index, indexableInfos).Wait();
        }

        protected void Handle()
        {
            OperationMonitor.Register(this.Run);
            OperationMonitor.Trigger();
        }

        protected List<QueuedEvent> ReadQueue(EventQueue eventQueue)
        {
            var queue = new List<QueuedEvent>();

            DateTime utcNow = DateTime.UtcNow;

            long? lastUpdatedTimestamp = this.index.Summary.LastUpdatedTimestamp;
            long fromTimestamp = lastUpdatedTimestamp == null || lastUpdatedTimestamp == 0L
                                ? this.GetLastPublishingTimestamp(eventQueue)
                                : lastUpdatedTimestamp.Value;
            var query = new EventQueueQuery { FromTimestamp = fromTimestamp, EventType = typeof(SavedItemRemoteEvent) };

            var toTimestamp = this.GetLastProcessedEventTimestamp(eventQueue);

            query.ToTimestamp = toTimestamp;

            queue.AddRange(eventQueue.GetQueuedEvents(query));

            // adding removed versions
            query.EventType = typeof(RemovedVersionRemoteEvent);
            queue.AddRange(eventQueue.GetQueuedEvents(query));

            // adding deleted items
            query.EventType = typeof(DeletedItemRemoteEvent);
            queue.AddRange(eventQueue.GetQueuedEvents(query));

            this.index.Summary.LastUpdated = utcNow;

            CrawlingLog.Log.Info(string.Format("SUPPORT [Index={0}] OnPublishEndAsynchronousStrategy: Processing queue [{1}; {2}] Count: {3}", this.index.Name, fromTimestamp, toTimestamp, queue.Count));

            return queue;
        }

        protected Dictionary<DataUri, DateTime> ExtractUrisFromQueue(List<QueuedEvent> queue)
        {
            var data = new Dictionary<DataUri, DateTime>();

            this.ProcessQueue(
                queue,
                (uri, queuedEvent) =>
                {
                    if (!data.ContainsKey(uri))
                    {
                        data.Add(uri, queuedEvent.Created);
                    }
                });

            return data;
        }

        protected Dictionary<DataUri, long> ExtractUrisAndTimestampFromQueue(List<QueuedEvent> queue)
        {
            var data = new Dictionary<DataUri, long>();

            this.ProcessQueue(
                queue,
                (uri, queuedEvent) =>
                {
                    if (!data.ContainsKey(uri))
                    {
                        data.Add(uri, queuedEvent.Timestamp);
                    }
                });

            return data;
        }

        protected IEnumerable<IndexableInfo> ExtractIndexableInfoFromQueue(List<QueuedEvent> queue)
        {
            return this.ExtractUrisAndTimestampFromQueue(queue)
                .Select(de => new IndexableInfo(new SitecoreItemUniqueId(new ItemUri(de.Key.ItemID, de.Key.Language, de.Key.Version, this.Database)), de.Value))
                .GroupBy(x => x.IndexableUniqueId)
                .Select(group => group.Single(x => x.Timestamp == group.Max(i => i.Timestamp)))
                .OrderBy(x => x.Timestamp).ToList();
        }

        protected virtual long GetLastPublishingTimestamp([NotNull] EventQueue eventQueue)
        {
            Assert.ArgumentNotNull(eventQueue, "eventQueue");
            var query = new EventQueueQuery { EventType = typeof(PublishEndRemoteEvent) };
            List<QueuedEvent> events = eventQueue.GetQueuedEvents(query).ToList();

            if (events.Count > 1)
            {
                return events.OrderByDescending(evt => evt.Timestamp).Skip(1).First().Timestamp;
            }

            return 0L;
        }

        private void ProcessQueue(IEnumerable<QueuedEvent> queue, Action<DataUri, QueuedEvent> addElement)
        {
            var serializer = new Serializer();
            foreach (var queuedEvent in queue)
            {
                var instanceData = serializer.Deserialize<SavedItemRemoteEvent>(queuedEvent.InstanceData);

                if (instanceData == null)
                {
                    continue;
                }

                var uri = new DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Sitecore.Data.Version.Parse(instanceData.VersionNumber));

                addElement(uri, queuedEvent);
            }
        }

        protected virtual long? GetLastProcessedEventTimestamp(EventQueue eventQueue)
        {
            var  eqEx = eventQueue as SqlServerEventQueue;
            return eqEx?.GetLastProcessedStamp;
        }
    }
}