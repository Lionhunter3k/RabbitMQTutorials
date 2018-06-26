using System;
using System.Collections.Generic;
using System.Text;

namespace Domain
{
    public class DisposableLinkedList<T> : LinkedList<T>, IDisposable where T : IDisposable
    {
        public void Dispose()
        {
            while(this.Count > 0)
            {
                var current = this.First;
                current.Value.Dispose();
                this.RemoveFirst();
            }
        }
    }
}
