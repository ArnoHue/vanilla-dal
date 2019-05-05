namespace VanillaDAL {
	public class Parameter {
		private string name;
		private object val;

		public Parameter(string _name, object _val) {
			name = _name;
			val = _val;
		}

		public object Value {
			get { return val; }
			set { val = value; }
		}

		public string Name {
			get { return name; }
		}
	}
}