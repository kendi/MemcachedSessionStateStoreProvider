using System;
using System.Collections.Specialized;
using System.IO;
using System.Web;
using System.Web.Configuration;
using System.Web.SessionState;
using Enyim.Caching;
using Enyim.Caching.Memcached;

namespace Hoge
{
	[Serializable]
	public class SessionItem
	{
		public DateTime CreatedAt { get; set; }
		public DateTime LockDate { get; set; }
		public int LockID { get; set; }
		public int Timeout { get; set; }
		public bool Locked { get; set; }
		public byte[] SessionItems { get; set; }
		public int ActionFlags { get; set; }
	}

	public class MemcachedSessionStateStoreProvider : SessionStateStoreProviderBase
	{
		private TimeSpan _timeout;

		public MemcachedClient MemcachedClient
		{
			get { return new MemcachedClient(); }
		}

		public override void Dispose()
		{
		}

		public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
		{
			return false;
		}

		public override void Initialize(string name, NameValueCollection config)
		{
			if(config == null)
				throw new ArgumentNullException("config");

			if (string.IsNullOrEmpty(name))
				name = "MemcachedSessionStateStoreProvider";

			base.Initialize(name, config);

			var sessionStateConfig = (SessionStateSection)WebConfigurationManager.GetSection("system.web/sessionState");
			_timeout = sessionStateConfig.Timeout;
		}

		public override void InitializeRequest(HttpContext context)
		{
		}

		public override SessionStateStoreData GetItem(
			HttpContext context,
			string id,
			out bool locked,
			out TimeSpan lockAge,
			out object lockId,
			out SessionStateActions actions)
		{
			return this.GetSessionItem(false, context, id, out locked, out lockAge, out lockId, out actions);
		}

		public override SessionStateStoreData GetItemExclusive(
			HttpContext context,
			string id,
			out bool locked,
			out TimeSpan lockAge,
			out object lockId,
			out SessionStateActions actions)
		{
			return this.GetSessionItem(true, context, id, out locked, out lockAge, out lockId, out actions);
		}

		private static object _mutex = new object();
		private SessionStateStoreData GetSessionItem(
			bool lockItem,
			HttpContext context,
			string id,
			out bool locked,
			out TimeSpan lockAge,
			out object lockId,
			out SessionStateActions actions
		)
		{
			// initialize out parameter
			locked = false;
			lockAge = TimeSpan.Zero;
			lockId = null;
			actions = SessionStateActions.None;

			using (var c = this.MemcachedClient)
			{
				if (lockItem)
				{
					lock (_mutex)
					{
						item = c.Get<SessionItem>(id);

						if (item == null) return null;

						var otherSessionlocked = this.LockSessionItem(id, item);
						if (otherSessionlocked)
						{
							locked = true;
							lockAge = GetLockAge(item);
							return null; // session item is not found or other thread locked 
						}	
					}
				}

				item = c.Get<SessionItem>(id);

				// setting out parameter
				lockId = ++item.LockID;
				lockAge = this.GetLockAge(item);
				actions = (SessionStateActions)item.ActionFlags;

				item.ActionFlags = (int)SessionStateActions.None;
				c.Store(StoreMode.Replace, id, item, _timeout);

				return actions == SessionStateActions.InitializeItem
					? this.CreateNewStoreData(context, 1)
					: Deserialize(context, item.SessionItems, item.Timeout);
			}
		}

		private TimeSpan GetLockAge(SessionItem item)
		{
			return DateTime.UtcNow.Subtract(item.LockDate);
		}

		private bool LockSessionItem(string id, SessionItem currentItem)
		{
			if(currentItem == null)
				throw new ArgumentNullException("currentItem");

			if (currentItem.Locked)
				return false;

			using(var c = this.MemcachedClient)
			{
				currentItem.Locked = true;
				currentItem.LockDate = DateTime.UtcNow;
				c.Store(StoreMode.Replace, id, currentItem, _timeout);
				return true;
			}
		}

		public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
		{
			using (var c = this.MemcachedClient)
			{
				var currentItem = c.Get<SessionItem>(id);
				if (currentItem == null || currentItem.LockID != (int) lockId)
					return;
				
				currentItem.Locked = false;
				c.Store(StoreMode.Replace, id, currentItem, _timeout);
			}
		}

		public override void SetAndReleaseItemExclusive(
			HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
		{
			using (var c = this.MemcachedClient)
			{
				var serialized = this.Serialize((SessionStateItemCollection)item.Items);
				if (newItem)
				{
					var setValue = this.CreateUninitializedItem(id, 1);
					setValue.SessionItems = serialized;
					c.Store(StoreMode.Add, id, setValue, _timeout);
				}
				else
				{
					var currentItem = c.Get<SessionItem>(id);
					
					if(currentItem == null || currentItem.LockID != (int)lockId) return;

					currentItem.Locked = false;
					currentItem.SessionItems = serialized;
					c.Store(StoreMode.Replace, id, currentItem, _timeout);
				}
			}
		}

		public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
		{
			using (var c = this.MemcachedClient)
			{
				var val = c.Get(id);
				if (val != null && ((SessionItem)val).LockID == (int)lockId)
					c.Remove(id);
			}
		}

		public override void ResetItemTimeout(HttpContext context, string id)
		{
		}

		public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
		{
			return new SessionStateStoreData(
				new SessionStateItemCollection(), 
				SessionStateUtility.GetSessionStaticObjects(context),
				timeout);
		}

		public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
		{
			using (var c = this.MemcachedClient)
			{
				c.Store(StoreMode.Add, id, this.CreateUninitializedItem(id, timeout), _timeout);
			}
		}

		private SessionItem CreateUninitializedItem(string id, int timeout)
		{
			return new SessionItem
			{
				CreatedAt = DateTime.Now.ToUniversalTime(),
				ActionFlags = 0,
				LockDate = DateTime.Now.ToUniversalTime(),
				Locked = false,
				LockID = 0,
				SessionItems = new byte[0],
				Timeout = timeout
			};
		}

		public override void EndRequest(HttpContext context)
		{
		}

		private byte[] Serialize(SessionStateItemCollection item)
		{
			using (var ms = new MemoryStream())
			using (var writer = new BinaryWriter(ms))
			{
				if (item != null)
					item.Serialize(writer);

				writer.Close();

				return ms.ToArray();
			}
		}

		private SessionStateStoreData Deserialize(HttpContext context, byte[] serializedItem, int timeout)
		{
			if(serializedItem == null)
				throw new ArgumentNullException("serializedItem");

			using (var ms = new MemoryStream(serializedItem))
			{
				var sessionItems = new SessionStateItemCollection();
				if (ms.Length > 0)
				{
					using (var reader = new BinaryReader(ms))
					{
						sessionItems = SessionStateItemCollection.Deserialize(reader);
					}
				}

				return new SessionStateStoreData(
					sessionItems,
					SessionStateUtility.GetSessionStaticObjects(context),
					timeout);
			}
		}
	}
}