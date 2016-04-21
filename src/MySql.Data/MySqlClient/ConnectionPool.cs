using System;
using System.Collections.Generic;
using MySql.Data.Serialization;

namespace MySql.Data.MySqlClient
{
	internal sealed class ConnectionPool
	{
		public string ConnectionString { get; }

		public string DataSource { get; }

		public string[] Hostnames { get; }

		public int Port { get; }

		public string UserID { get; }

		public string Password { get; }

		public string Database { get; }

		public bool ConnectionReset { get; }

		public bool AllowUserVariables { get; }

		public bool ConvertZeroDateTime { get; }

		public bool OldGuids { get; }

		public MySqlSession TryGetSession()
		{
			lock (m_lock)
			{
				if (m_sessions.Count > 0)
					return m_sessions.Dequeue();
			}
			return null;
		}

		public bool Return(MySqlSession session)
		{
			lock (m_lock)
			{
				// TODO: Dispose oldest connections in the pool first?
				if (m_sessions.Count >= m_maximumSize)
					return false;
				m_sessions.Enqueue(session);
				return true;
			}
		}

		public static ConnectionPool GetPool(string connectionString)
		{
			lock (s_lock)
			{
				ConnectionPool pool;
				if (!s_pools.TryGetValue(connectionString, out pool))
				{
					pool = new ConnectionPool(connectionString);
					s_pools.Add(connectionString, pool);
				}
				return pool;
			}
		}

		public static void ClearPools()
		{
			List<ConnectionPool> pools;
			lock (s_lock)
				pools = new List<ConnectionPool>(s_pools.Values);

			foreach (var pool in pools)
				pool.Clear();
		}

		private ConnectionPool(string connectionString)
		{
			var csb = new MySqlConnectionStringBuilder(connectionString);

			if (csb.UseCompression)
				throw new NotSupportedException("Compression not supported.");

			m_lock = new object();
			m_sessions = new Queue<MySqlSession>();
			m_maximumSize = csb.Pooling ? (int) csb.MaximumPoolSize : 0;

			DataSource = csb.Server;
			Hostnames = csb.Server.Split(',');
			Port = (int) csb.Port;
			UserID = csb.UserID;
			Password = csb.Password;
			Database = csb.Database;
			ConnectionReset = csb.ConnectionReset;
			AllowUserVariables = csb.AllowUserVariables;
			ConvertZeroDateTime = csb.ConvertZeroDateTime;
			OldGuids = csb.OldGuids;

			if (!csb.PersistSecurityInfo)
			{
				foreach (string key in csb.Keys)
					foreach (var passwordKey in MySqlConnectionStringOption.Password.Keys)
						if (string.Equals(key, passwordKey, StringComparison.OrdinalIgnoreCase))
							csb.Remove(key);
			}
			ConnectionString = csb.ConnectionString;
		}

		private void Clear()
		{
			lock (m_lock)
			{
				while (m_sessions.Count > 0)
				{
					var session = m_sessions.Dequeue();
					session.Dispose();
				}
			}
		}

		static readonly object s_lock = new object();
		static readonly Dictionary<string, ConnectionPool> s_pools = new Dictionary<string, ConnectionPool>();

		readonly object m_lock;
		readonly Queue<MySqlSession> m_sessions;
		readonly int m_maximumSize;
	}
}
