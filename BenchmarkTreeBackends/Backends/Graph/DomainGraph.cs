using System;
using System.Collections.Concurrent;

namespace BenchmarkTreeBackends.Backends.Graph
{
    public class DomainGraph<T> : GraphDnsBackend<string, T> where T : class
    {
        #region variables

        private static readonly byte[] _keyMap;
        private static readonly byte[] _reverseKeyMap;

        #endregion variables

        static DomainGraph()
        {
            _keyMap = new byte[256];
            _reverseKeyMap = new byte[41];

            int keyCode;

            for (int i = 0; i < _keyMap.Length; i++)
            {
                if (i == 46) //[.]
                {
                    keyCode = 0;
                    _keyMap[i] = (byte)keyCode;
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if (i == 42) //[*]
                {
                    keyCode = 1;
                    _keyMap[i] = 0xff; //skipped value for optimization
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if (i == 45) //[-]
                {
                    keyCode = 2;
                    _keyMap[i] = (byte)keyCode;
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if (i == 47) //[/]
                {
                    keyCode = 3;
                    _keyMap[i] = (byte)keyCode;
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if (i >= 48 && i <= 57) //[0-9]
                {
                    keyCode = i - 44; //4 - 13
                    _keyMap[i] = (byte)keyCode;
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if (i == 95) //[_]
                {
                    keyCode = 14;
                    _keyMap[i] = (byte)keyCode;
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if (i >= 97 && i <= 122) //[a-z]
                {
                    keyCode = i - 82; //15 - 40
                    _keyMap[i] = (byte)keyCode;
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if (i >= 65 && i <= 90) //[A-Z]
                {
                    keyCode = i - 50; //15 - 40
                    _keyMap[i] = (byte)keyCode;
                }
                else
                {
                    _keyMap[i] = 0xff;
                }
            }
        }

        public void Add(string key, T value)
        {
            base.Add(key, new DnsZoneNode<T>(value));
        }

        public override byte[]? ConvertToByteKey(string domain, bool throwException = true)
        {
            if (domain is null)
            {
                if (throwException) throw new ArgumentNullException(nameof(domain));
                return null;
            }

            if (domain.Length == 0)
                return [];

            if (domain.Length > 255)
            {
                if (throwException)
                    throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: length cannot exceed 255 bytes.");

                return null;
            }

            byte[] key = new byte[domain.Length + 1];
            int keyOffset = 0;
            int labelStart;
            int labelEnd = domain.Length - 1;
            int labelLength;
            int labelChar;
            byte labelKeyCode;
            int i;

            do
            {
                if (labelEnd < 0)
                    labelEnd = 0;

                labelStart = domain.LastIndexOf('.', labelEnd);
                labelLength = labelEnd - labelStart;

                if (labelLength == 0)
                {
                    if (throwException)
                        throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: label length cannot be 0 byte.");

                    return null;
                }

                if (labelLength > 63)
                {
                    if (throwException)
                        throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: label length cannot exceed 63 bytes.");

                    return null;
                }

                if (domain[labelStart + 1] == '-')
                {
                    if (throwException)
                        throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: label cannot start with hyphen.");

                    return null;
                }

                if (domain[labelEnd] == '-')
                {
                    if (throwException)
                        throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: label cannot end with hyphen.");

                    return null;
                }

                if (labelLength == 1 && domain[labelStart + 1] == '*') //[*]
                {
                    key[keyOffset++] = 1;
                }
                else
                {
                    for (i = labelStart + 1; i <= labelEnd; i++)
                    {
                        labelChar = domain[i];
                        if (labelChar >= _keyMap.Length)
                        {
                            if (throwException)
                                throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: invalid character [" + labelChar + "] was found.");

                            return null;
                        }

                        labelKeyCode = _keyMap[labelChar];
                        if (labelKeyCode == 0xff)
                        {
                            if (throwException)
                                throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: invalid character [" + labelChar + "] was found.");

                            return null;
                        }

                        key[keyOffset++] = labelKeyCode;
                    }
                }

                key[keyOffset++] = 0; //[.]
                labelEnd = labelStart - 1;
            }
            while (labelStart > -1);

            return key;
        }
        
        public override bool TryGet(string key, out T value)
        {
            value = null;
            if (_nodes.TryGetValue(key, out var nodeValue))
            {
                value = nodeValue as T;
                    ;
                return true;
            }
            return false;
        }

        protected override void IndexReverseRecords(string name, DnsZoneNode<T> node)
        {
            foreach (var kvp in node.Records)
            {
                if (kvp.Key == RecordType.A || kvp.Key == RecordType.AAAA)
                {
                    foreach (var ip in kvp.Value)
                    {
                        _reverseIndex.GetOrAdd(ip.ToString(), _ => new ConcurrentBag<T>()).Add(name);
                    }
                }
                else if (kvp.Key == RecordType.PTR)
                {
                    foreach (var target in kvp.Value)
                    {
                        _reverseIndex.GetOrAdd(name, _ => new ConcurrentBag<T>()).Add(target);
                    }
                }
            }
        }


        protected void UnindexReverseRecords(string name, DnsZoneNode<string> node)
        {
            foreach (var kvp in node.Records)
            {
                if (kvp.Key == RecordType.A || kvp.Key == RecordType.AAAA)
                {
                    foreach (var ip in kvp.Value)
                    {
                        if (_reverseIndex.TryGetValue(ip, out var bag))
                            while (bag.TryTake(out _) && bag.IsEmpty) { }
                    }
                }
            }
        }

        protected override void UnindexReverseRecords(string name, DnsZoneNode<T> node)
        {
            throw new NotImplementedException();
        }
    }
}