using System;
using VanillaDAL.Config;

namespace VanillaDAL {
	public abstract class VanillaException : ApplicationException {
		protected VanillaException() {
		}

		protected VanillaException(string message)
			: base(message) {
		}

		protected VanillaException(string message, Exception inner)
			: base(message, inner) {
		}
	}

	public class VanillaConfigException : VanillaException {
		public VanillaConfigException() {
		}

		public VanillaConfigException(string message)
			: base(message) {
		}

		public VanillaConfigException(string message, Exception inner)
			: base(message, inner) {
		}
	}

	public class VanillaExecutionException : VanillaException {
		public VanillaExecutionException() {
		}

		public VanillaExecutionException(string message)
			: base(message) {
		}

		public VanillaExecutionException(string message, Exception inner)
			: base(message, inner) {
		}
	}

	public class VanillaConcurrencyException : VanillaException {
		public VanillaConcurrencyException() {
		}

		public VanillaConcurrencyException(string message)
			: base(message) {
		}

		public VanillaConcurrencyException(string message, Exception inner)
			: base(message, inner) {
		}
	}

	public class VanillaFactory {
		private VanillaFactory() {
		}

		public static IDBAccessor CreateDBAccessor(VanillaConfig config) {
			switch (config.DatabaseType) {
				case ConfigDatabaseType.SQLServer:
					return new DBAccessorSQLServer(config);
				case ConfigDatabaseType.Oracle:
					return new DBAccessorOracle(config);
				case ConfigDatabaseType.OLEDB:
					return new DBAccessorOLEDB(config);
				default:
					return null;
			}
		}
	}
}