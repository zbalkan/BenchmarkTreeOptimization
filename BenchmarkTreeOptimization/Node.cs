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

using System;
using System.Threading;

namespace BenchmarkTreeOptimization
{

    public sealed class Node<T> where T : class
    {
        #region variables

        private readonly Node<T>? _parent = null;
        private readonly int _depth;
        private readonly byte _k;

        private readonly Node<T>[]? _children;
        private volatile NodeValue<T>? _value;

        #endregion variables

        #region constructor

        public Node(Node<T>? parent, byte k, int keySpace, NodeValue<T>? value)
        {
            if (parent is null)
            {
                _depth = 0;
                _k = 0;
            }
            else
            {
                _parent = parent;
                _depth = _parent._depth + 1;
                _k = k;
            }

            if (keySpace > 0)
                _children = new Node<T>[keySpace];

            _value = value;

            if ((_children is null) && (_value is null))
                throw new InvalidOperationException();
        }

        #endregion constructor

        #region private

        private static bool KeyEquals(int startIndex, byte[] key1, byte[] key2)
        {
            if (key1.Length != key2.Length)
                return false;

            for (int i = startIndex; i < key1.Length; i++)
            {
                if (key1[i] != key2[i])
                    return false;
            }

            return true;
        }

        #endregion private

        #region public

        public bool AddNodeValue(byte[]? key, Func<NodeValue<T>> newValue, int keySpace, out NodeValue<T>? addedValue, out NodeValue<T>? existingValue)
        {
            ArgumentNullException.ThrowIfNull(key);

            Node<T> current = this;

            do //try again loop
            {
                while (current._depth < key.Length) //find loop
                {
                    if (current._children is null)
                        break;

                    byte k = key[current._depth];
                    Node<T> child = Volatile.Read(ref current._children[k]);
                    if (child is null)
                    {
                        //try set new leaf node with add value in this empty spot
                        Node<T> addNewNode = new Node<T>(current, k, 0, newValue());
                        Node<T> originalChild = Interlocked.CompareExchange(ref current._children[k], addNewNode, null);
                        if (originalChild is null)
                        {
                            //value added as leaf node
                            addedValue = addNewNode._value;
                            existingValue = null;
                            return true;
                        }

                        //another thread already added a child; use that reference
                        child = originalChild;
                    }

                    current = child;
                }

                //either current is leaf or key belongs to current
                NodeValue<T>? value = current._value;

                if ((value is not null) && KeyEquals(current._depth, value.Key, key))
                {
                    //value found; cannot add
                    addedValue = null;
                    existingValue = value;
                    return false;
                }
                else
                {
                    //value key does not match
                    if (current._children is null)
                    {
                        //current node is a leaf (has no children); convert it into stem node
                        Node<T> stemNode;

                        if (value.Key.Length == current._depth)
                        {
                            //current value belongs current leaf node
                            //replace current leaf node with a stem node with current value
                            stemNode = new Node<T>(current._parent, current._k, keySpace, value);
                        }
                        else
                        {
                            //current value does not belong to current leaf node
                            //replace current leaf node with a stem node with null value
                            stemNode = new Node<T>(current._parent, current._k, keySpace, null);

                            //copy current value into a child leaf node
                            byte k = value.Key[current._depth];
                            stemNode._children[k] = new Node<T>(stemNode, k, 0, value);
                        }

                        //replace stem node in parent
                        Node<T> originalNode = Interlocked.CompareExchange(ref current._parent._children[current._k], stemNode, current);
                        if (ReferenceEquals(originalNode, current))
                        {
                            //successfully added stem node
                            //use new stem node as current node and try again
                            current = stemNode;
                        }
                        else
                        {
                            //another thread already placed new stem node or removed it
                            if (originalNode is null)
                            {
                                //stem node was removed by another thread; start over again
                                current = this;
                            }
                            else
                            {
                                //use new stem node reference as current and try again
                                current = originalNode;
                            }
                        }
                    }
                    else
                    {
                        //current node is stem with no/invalid value; add value here
                        NodeValue<T> addNewValue = newValue();
                        NodeValue<T> originalValue = Interlocked.CompareExchange(ref current._value, addNewValue, value);
                        if (ReferenceEquals(originalValue, value))
                        {
                            //value added successfully
                            addedValue = addNewValue;
                            existingValue = null;
                            return true;
                        }

                        if (originalValue is not null)
                        {
                            //another thread added value to stem node; return its reference
                            addedValue = null;
                            existingValue = originalValue;
                            return false;
                        }

                        //another thread removed value; try again
                    }
                }
            }
            while (true);
        }

        public NodeValue<T>? FindNodeValue(byte[]? key, out Node<T> currentNode)
        {
            ArgumentNullException.ThrowIfNull(key);

            currentNode = this;

            while (currentNode._depth < key.Length) //find loop
            {
                if (currentNode._children is null)
                    break;

                Node<T> child = Volatile.Read(ref currentNode._children[key[currentNode._depth]]);
                if (child is null)
                    return null; //value not found

                currentNode = child;
            }

            //either currentNode is leaf or key belongs to currentNode
            NodeValue<T> value = currentNode._value;

            if ((value is not null) && KeyEquals(currentNode._depth, value.Key, key))
                return value; //value found

            return null; //value key does not match
        }

        public NodeValue<T>? RemoveNodeValue(byte[]? key, out Node<T>? currentNode)
        {
            currentNode = this;

            do //try again loop
            {
                while (currentNode._depth < key.Length) //find loop
                {
                    if (currentNode._children is null)
                        break;

                    Node<T> child = Volatile.Read(ref currentNode._children[key[currentNode._depth]]);
                    if (child is null)
                        return null; //value not found

                    currentNode = child;
                }

                //either currentNode is leaf or key belongs to currentNode
                NodeValue<T>? value = currentNode._value;

                if ((value is not null) && KeyEquals(currentNode._depth, value.Key, key))
                {
                    //value found; remove and return value
                    if (currentNode._children is null)
                    {
                        //remove leaf node directly from parent
                        Node<T> originalNode = Interlocked.CompareExchange(ref currentNode._parent._children[currentNode._k], null, currentNode);
                        if (ReferenceEquals(originalNode, currentNode))
                            return value; //leaf node removed successfully

                        if (originalNode is null)
                        {
                            //another thread removed leaf node
                            return null;
                        }
                        else
                        {
                            //another thread replaced leaf node with stem node; use new reference and try again in next iteration
                            currentNode = originalNode;
                        }
                    }
                    else
                    {
                        //remove value from stem node
                        NodeValue<T> originalValue = Interlocked.CompareExchange(ref currentNode._value, null, value);
                        if (ReferenceEquals(originalValue, value))
                            return value; //successfully removed stem node value

                        //another thread removed stem node value
                        return null;
                    }
                }
                else
                {
                    //value key does not match
                    return null;
                }
            }
            while (true);
        }

        public void CleanThisBranch()
        {
            Node<T> current = this;

            while (current._parent is not null)
            {
                if (current._children is null)
                {
                    //current node is leaf
                    //leaf node already was removed so move up to parent
                }
                else
                {
                    //current node is stem
                    if (!current.IsEmpty)
                        return;

                    //remove current node from parent
                    Volatile.Write(ref current._parent._children[current._k], null);
                }

                //make parent as current and proceed cleanup of parent node
                current = current._parent;
            }
        }

        public void ClearNode()
        {
            //remove value
            _value = null;

            if (_children is not null)
            {
                //remove all children
                for (int i = 0; i < _children.Length; i++)
                    if (_children[i] is not null)
                        Volatile.Write(ref _children[i], null);
            }
        }

        public Node<T> GetNextNodeWithValue(int baseDepth)
        {
            int k = 0;
            Node<T> current = this;

            while ((current is not null) && (current._depth >= baseDepth))
            {
                if (current._children is not null)
                {
                    //find child node
                    Node<T>? child = null;

                    for (int i = k; i < current._children.Length; i++)
                    {
                        child = Volatile.Read(ref current._children[i]);
                        if (child is not null)
                        {
                            if (child._value is not null)
                                return child; //child has value so return it

                            if (child._children is not null)
                                break;
                        }
                    }

                    if (child is not null)
                    {
                        //make found child as current
                        k = 0;
                        current = child;
                        continue; //start over
                    }
                }

                //no child nodes available; move up to parent node
                k = current._k + 1;
                current = current._parent;
            }

            return null;
        }

        public Node<T>? GetLastNodeWithValue()
        {
            Node<T>? lastNode = null;
            Node<T> current = this;

            while (true)
            {
                if (current._value is not null)
                    lastNode = current;

                if (current._children is null)
                    break;

                for (int i = current._children.Length - 1; i > -1; i--)
                {
                    //find child node
                    Node<T> child = Volatile.Read(ref current._children[i]);
                    if (child is not null)
                    {
                        current = child;
                        break;
                    }
                }
            }

            return lastNode;
        }

        public Node<T>? GetPreviousNodeWithValue(int baseDepth)
        {
            int k = _k - 1;
            Node<T> current = _parent;

            while ((current is not null) && (current._depth >= baseDepth))
            {
                if (current._children is not null)
                {
                    //find child node
                    Node<T>? child = null;

                    for (int i = k; i > -1; i--)
                    {
                        child = Volatile.Read(ref current._children[i]);
                        if (child is not null)
                        {
                            if (child._children is not null)
                                break; //child has further children so check them first

                            if (child._value is not null)
                                return child; //child has value so return it
                        }
                    }

                    if (child is not null)
                    {
                        //make found child as current
                        k = current._children.Length - 1;
                        current = child;
                        continue; //start over
                    }
                }

                //no child nodes available
                if (current._value is not null)
                    return current; //current node has value so return i

                //move up to parent node for previous sibling
                k = current._k - 1;
                current = current._parent;
            }

            return null;
        }

        #endregion public

        #region properties

        public Node<T> Parent
        { get { return _parent; } }

        public int Depth
        { get { return _depth; } }

        public byte K
        { get { return _k; } }

        public Node<T>[] Children
        { get { return _children; } }

        public NodeValue<T> Value
        { get { return _value; } }

        public bool IsEmpty
        {
            get
            {
                if (_value is not null)
                    return false;

                if (_children is not null)
                {
                    for (int i = 0; i < _children.Length; i++)
                    {
                        if (Volatile.Read(ref _children[i]) is not null)
                            return false;
                    }
                }

                return true;
            }
        }

        public bool HasChildren
        {
            get
            {
                if (_children is null)
                    return false;

                for (int i = 0; i < _children.Length; i++)
                {
                    if (Volatile.Read(ref _children[i]) is not null)
                        return true;
                }

                return false;
            }
        }

        #endregion properties
    }
}
