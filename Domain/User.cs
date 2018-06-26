using System;
using System.Collections.Generic;

namespace Domain
{
    public class User
    {
        public string UserName { get; set; }

        public string Email { get; set; }

        public DateTime DOB { get; set; } = DateTime.Now;

        public List<Address> Addresses { get; set; } = new List<Address>();
    }

    public class Address
    {
        public string Street { get; set; }

        public long Number { get; set; }
    }
}
