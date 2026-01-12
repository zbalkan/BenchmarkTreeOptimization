/*
Technitium DNS Server
Copyright (C) 2025  Shreyas Zare (shreyas@technitium.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

using System.Text;

namespace BenchmarkTreeOptimization

{
    public class OptimizedDomainTree<T> : ByteTree<string, T> where T : class
    {
        #region variables

        private static readonly byte[] _keyMap;
        private static readonly byte[] _reverseKeyMap;

        #endregion variables

        #region constructor

        static OptimizedDomainTree()
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
                else if ((i >= 48) && (i <= 57)) //[0-9]
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
                else if ((i >= 97) && (i <= 122)) //[a-z]
                {
                    keyCode = i - 82; //15 - 40
                    _keyMap[i] = (byte)keyCode;
                    _reverseKeyMap[keyCode] = (byte)i;
                }
                else if ((i >= 65) && (i <= 90)) //[A-Z]
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

        public OptimizedDomainTree()
            : base(41)
        { }

        #endregion constructor

        #region protected

        public override byte[]? ConvertToByteKey(string domain, bool throwException = true)
        {
            if (domain.Length == 0)
                return [];

            if (domain[^1] == '.')
            {
                if (throwException)
                    throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: label length cannot be 0 byte.");
                return null;
            }

            if (domain.Length > 255)
            {
                if (throwException)
                    throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: length cannot exceed 255 bytes.");

                return null;
            }

            // Worst case: every label adds 1 length byte, so encoded length can exceed domain.Length.
            // Allocate enough for speed (stack) and enforce final <= 255.
            Span<byte> key = stackalloc byte[Math.Min(512, domain.Length * 2)];
            int keyPos = 0, strPos = 0;
            int labelChar;
            byte labelKeyCode;

            while (strPos < domain.Length)
            {
                int labelStart = strPos;
                while (strPos < domain.Length && domain[strPos] != '.') strPos++;

                int labelLength = strPos - labelStart;
                int labelEnd = strPos - 1;

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

                // Correct "starts with hyphen" check (bug fix vs domain[labelStart + 1])
                if (domain[labelStart] == '-')
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

                // Ensure capacity
                if (keyPos + 1 + labelLength > key.Length)
                {
                    // Fallback to heap if extremely pathological; still enforce <=255 below.
                    byte[] tmp = new byte[(domain.Length + 1) * 2];
                    key.CopyTo(tmp);
                    key = tmp;
                }

                // Single-label wildcard support like DefaultDomainTree
                if (labelLength == 1 && domain[labelStart] == '*')
                {
                    key[keyPos++] = 1;   // label length = 1
                    key[keyPos++] = 1;   // wildcard token (same as DefaultDomainTree key code)
                }
                else
                {
                    key[keyPos++] = (byte)labelLength;

                    for (int i = labelStart; i < strPos; i++)
                    {
                        labelChar = domain[i];
                        if ((uint)labelChar >= (uint)_keyMap.Length)
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

                        key[keyPos++] = labelKeyCode;
                    }
                }

                if (strPos < domain.Length) strPos++; // skip '.'
            }

            // Enforce DNS max name length in bytes (wire format).
            if (keyPos > 255)
            {
                if (throwException)
                    throw new InvalidDomainNameException("Invalid domain name [" + domain + "]: encoded length cannot exceed 255 bytes.");
                return null;
            }

            return key.Slice(0, keyPos).ToArray();
        }

        protected static string? ConvertKeyToLabel(byte[] key, int startIndex)
        {
            int length = key.Length - startIndex;
            if (length < 1)
                return null;

            // Wire format: [len][label-bytes...]
            int len = key[startIndex];
            if (len == 0)
                return string.Empty;

            if (len < 0 || len > 63 || length < 1 + len)
                return null;

            Span<byte> label = stackalloc byte[len];
            for (int i = 0; i < len; i++)
            {
                int k = key[startIndex + 1 + i];
                if ((uint)k >= (uint)_reverseKeyMap.Length)
                    return null;

                label[i] = _reverseKeyMap[k];
            }

            return Encoding.ASCII.GetString(label);
        }

        #endregion protected

        #region public

        public override bool TryRemove(string key, out T? value)
        {
            if (TryRemove(key, out value, out Node? currentNode))
            {
                currentNode!.CleanThisBranch();
                return true;
            }

            return false;
        }

        #endregion public
    }
}