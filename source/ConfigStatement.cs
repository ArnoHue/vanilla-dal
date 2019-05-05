using System;
using System.Xml.Serialization;

namespace VanillaDAL.Config {
	public enum ConfigStatementType {
		Undefined,
		Text,
		StoredProcedure
	}

	[XmlRoot(ElementName = "Statement")]
	public class ConfigStatement {
		private string id;
		private ConfigStatementType statementType;
		private string code;
		private ConfigParameter[] parameters;

		public ConfigStatement() {
		}

		public ConfigStatement(ConfigStatementType _statementType, string _code)
			: this(_statementType, _code, new ConfigParameter[0]) {
		}

		public ConfigStatement(ConfigStatementType _statementType, string _code,
		                       ConfigParameter[] _parameters) {
			id = Guid.NewGuid().ToString();
			statementType = _statementType;
			code = _code;
			parameters = _parameters;
		}

		public string ID {
			get {
				if (id == null) {
					throw new VanillaConfigException("[Statement.ID] has not been set");
				}
				return id;
			}
			set { id = value; }
		}

		public ConfigStatementType StatementType {
			get {
				if (statementType == ConfigStatementType.Undefined) {
					throw new VanillaConfigException("[Statement.StatementType] has not been set");
				}
				return statementType;
			}
			set { statementType = value; }
		}


		public string Code {
			get {
				if (code == null) {
					throw new VanillaConfigException("[Statement.Code] has not been set");
				}
				return code;
			}
			set { code = value; }
		}


		[XmlArrayItem(ElementName = "Parameter")]
		public ConfigParameter[] Parameters {
			get {
				return parameters;
			}
			set { parameters = value; }
		}
	}
}