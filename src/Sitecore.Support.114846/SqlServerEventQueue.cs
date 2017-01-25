
namespace Sitecore.Support.Data.Eventing
{
    using Sitecore.Data;
    using Sitecore.Data.DataProviders.Sql;

    public class SqlServerEventQueue: Sitecore.Data.Eventing.SqlServerEventQueue
    {
        public SqlServerEventQueue(SqlDataApi api) : base(api)
        {
        }

        public SqlServerEventQueue(SqlDataApi api, Database database) : base(api, database)
        {
        }

        public virtual long? GetLastProcessedStamp
        {
            get { return this.timestamp?.Sequence; }
        }
    }
}