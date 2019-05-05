using System.Collections;

namespace VanillaDAL {
	public class ParameterList {
		private Hashtable parameters;

		public ParameterList(params Parameter[] _parameters) {
			parameters = new Hashtable();
			foreach (Parameter param in _parameters) {
				parameters[param.Name] = param;
			}
		}

		public ICollection Names {
			get { return parameters.Keys; }
		}

		public ICollection Parameters {
			get { return parameters.Values; }
		}

		public bool ContainsParameter(string name) {
			return parameters.ContainsKey(name);
		}

		public Parameter GetParameter(string name) {
			return (Parameter) parameters[name];
		}
	}
}