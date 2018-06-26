using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Domain
{
    public delegate (string typeName, Type type) TypeSelector(string typeName = null, Type type = null);

    public static class TransportExtensions
    {
        private static (string typeName, Type type) DefaultTypeSelector(string typeName = null, Type type = null)
        {
            if (typeName != null)
                type = Type.GetType($"{typeName}, Domain");
            if (type != null)
                typeName = type.FullName;
            return (typeName, type);
        }

        public static JsonSerializerSettings Settings = new JsonSerializerSettings();

        public static TypeSelector TypeSelector = DefaultTypeSelector;

        private unsafe static void SetBytes(short value, byte[] bytes)
        {
            fixed (byte* b = bytes)
                *((short*)b) = value;
        }

        public static byte[] ToRawBody(this object input, out string message, TypeSelector typeSelector = null)
        {
            if (typeSelector == null)
                typeSelector = DefaultTypeSelector;
            message = JsonConvert.SerializeObject(input, Formatting.Indented, Settings);
            var (typeName, _) = typeSelector(type: input.GetType());
            var typeNameSize = (short)Encoding.UTF8.GetByteCount(typeName);
            var bodySize = Encoding.UTF8.GetByteCount(message);
            var rawBody = new byte[2 + typeNameSize + bodySize];
            SetBytes(typeNameSize, rawBody);
            Encoding.UTF8.GetBytes(typeName, 0, typeName.Length, rawBody, 2);
            Encoding.UTF8.GetBytes(message, 0, message.Length, rawBody, 2 + typeNameSize);
            return rawBody;
        }

        public static object FromRawBody(this byte[] body, out string message, TypeSelector typeSelector = null)
        {
            if (typeSelector == null)
                typeSelector = DefaultTypeSelector;
            var typeNameSize = BitConverter.ToInt16(body, 0);
            var typeName = Encoding.UTF8.GetString(body, 2, typeNameSize);
            message = Encoding.UTF8.GetString(body, 2 + typeNameSize, body.Length - 2 - typeNameSize);
            var (_, type) = typeSelector(typeName);
            var input = JsonConvert.DeserializeObject(message, type, Settings);
            return input;
        }
    }
}
