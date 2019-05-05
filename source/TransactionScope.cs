using System;
using System.Data;
using System.Threading;

namespace VanillaDAL
{
	/// <summary>
	/// Summary description for TransactionScope.
	/// </summary>
	internal class TransactionScope {

		private const string TRANSACTION_SLOT = "VanillaDAL.TransactionScope.TransactionSlot";

        internal static void SetCurrentTransaction(IDbTransaction tx) {
            if (tx != null && GetCurrentSlotData() != null) {
                throw new VanillaExecutionException("Active transaction exists already");
            }

            SetCurrentSlotData(tx);
        }

        internal static IDbTransaction GetCurrentTransaction() {
            return GetCurrentSlotData();
        }

		private static IDbTransaction GetCurrentSlotData() {
			 return (IDbTransaction)Thread.GetData(Thread.GetNamedDataSlot(TRANSACTION_SLOT));
		}

		private static void SetCurrentSlotData(IDbTransaction tx) {
			Thread.SetData(Thread.GetNamedDataSlot(TRANSACTION_SLOT), tx);
		}

	}
}
