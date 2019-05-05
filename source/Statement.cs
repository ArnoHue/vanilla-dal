using VanillaDAL.Config;
using System;

namespace VanillaDAL {

	public class Statement {

		private string id;
		private ConfigStatement stmt;

		public Statement(string _configId) {
			id = _configId;
		}

        public Statement(ConfigStatement _stmt) {
            stmt = _stmt;
        }

        public Statement(ConfigStatementType _type, string _code) {
            stmt = new ConfigStatement(_type, _code, null);
        }

        internal bool IsParameterInjectionRequired() {
            return stmt != null && stmt.Parameters == null;
        }

        internal void InjectParameters(ParameterList _params) {
            if (!IsParameterInjectionRequired()) {
                throw new VanillaConfigException("Statement already has parameter list");
            }

            ConfigParameter[] confParam = new ConfigParameter[_params.Parameters.Count];
            int i = 0;
            foreach (Parameter param in _params.Parameters) {
                object val = param.Value;
                if (val == null) {
                    throw new VanillaExecutionException("Statement parameter can not be null");
                }
                confParam[i] = new ConfigParameter();
                confParam[i].Name = param.Name;
                confParam[i].Type = DBAccessorBase.GetConfigParameterType(val.GetType());
                i++;
            }
            stmt.Parameters = confParam;
        }

		internal ConfigStatement GetConfigStatement(VanillaConfig config) {
			if (stmt != null) {
                if (IsParameterInjectionRequired()) {
                    throw new VanillaConfigException("Statement does not have parameter list");
                }
				return stmt;
			}
			else {
				return config.GetStatement(id);
			}
		}
	}
}