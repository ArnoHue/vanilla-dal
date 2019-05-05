using System;
using System.Collections;
using System.IO;
using System.Xml.Serialization;
using VanillaDAL.Config;

namespace VanillaDAL {
	public class VanillaConfig {
		private Hashtable statements = new Hashtable();
		private Configuration config;

		private VanillaConfig(Configuration _config, string connStr) {
			config = _config;
			foreach (ConfigStatement statement in _config.Statements) {
				statements[statement.ID] = statement;
			}
			config.ConnectionString = connStr;
		}

		public ConfigStatement GetStatement(string id) {
			if (!statements.ContainsKey(id)) {
				throw new VanillaExecutionException(string.Format("Statement [{0}] does not exist.", id));
			}
			return (ConfigStatement) statements[id];
		}

		public bool LogSql {
			get { return config.LogSql; }
		}

		internal ConfigDatabaseType DatabaseType {
			get { return config.DatabaseType; }
		}

		internal string ConnectionString {
			get { return config.ConnectionString; }
		}

		public static VanillaConfig CreateConfig(Stream inStream, string connStr) {
			try {
				XmlSerializer ser = new XmlSerializer(typeof (Configuration));
				VanillaConfig config = new VanillaConfig((Configuration) ser.Deserialize(inStream), connStr);
				return config;
			}
			catch (VanillaException) {
				throw;
			}
			catch (Exception ex) {
				throw new VanillaConfigException(ex.Message, ex);
			}
		}
	}
}