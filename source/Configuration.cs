using System.Xml.Serialization;

namespace VanillaDAL.Config {
	public enum ConfigDatabaseType {
		Undefined,
		SQLServer,
		Oracle,
		OLEDB
	}

	public enum ConfigLockingType {
		Undefined,
		NoLocking,
		OptimisticLocking
	}

	[XmlRoot(ElementName = "Config")]
	public class Configuration {
		private ConfigStatement[] statements;
		private ConfigDatabaseType dbType;
		private string connString;
		private bool logSql;

		public string ConnectionString {
			get {
				if (connString == null) {
					throw new VanillaConfigException("[Config.ConnectionString] has not been set");
				}
				return connString;
			}
			set { connString = value; }
		}


		public ConfigDatabaseType DatabaseType {
			get {
				if (dbType == ConfigDatabaseType.Undefined) {
					throw new VanillaConfigException("[Config.DatabaseType] has not been set");
				}
				return dbType;
			}
			set { dbType = value; }
		}

		public bool LogSql {
			get { return logSql; }
			set { logSql = value; }
		}

		[XmlArrayItem(ElementName = "Statement")]
		public ConfigStatement[] Statements {
			get {
				if (statements == null) {
					throw new VanillaConfigException("[Config.Statements] has not been set");
				}
				return statements;
			}
			set { statements = value; }
		}
	}
}