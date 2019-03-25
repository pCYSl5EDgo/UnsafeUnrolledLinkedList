using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;

namespace pcysl5edgo.Collections.LowLevel.Unsafe
{
    public unsafe struct NodeConstantCapacity
    {
        public IntPtr NextNode;
        private int length;
        public readonly void* Values;

        public int Length => length;
        public bool IsFull(int capacity) => length == capacity;

        public ref T GetRef<T>(int index) where T : unmanaged => ref ((T*)Values)[index];
        public ref T GetRef<T>(uint index) where T : unmanaged => ref ((T*)Values)[index];
        public ref T GetRef<T>(long index) where T : unmanaged => ref ((T*)Values)[index];
        public ref T GetRef<T>(ulong index) where T : unmanaged => ref ((T*)Values)[index];

        public NodeConstantCapacity(int capacity, int elementSize, Allocator allocator)
        {
            NextNode = IntPtr.Zero;
            length = 0;
            Values = UnsafeUtility.Malloc(capacity * elementSize, 4, allocator);
        }

        public static NodeConstantCapacity Create<T>(int capacity, Allocator allocator) where T : unmanaged => new NodeConstantCapacity(capacity, sizeof(T), allocator);

        public void Clear() => Interlocked.Exchange(ref length, 0);

        public void RemoveAtSwapBack<T>(int index) where T : unmanaged => GetRef<T>(index) = GetRef<T>(--length);

        public bool Contains<T>(ref T value) where T : unmanaged
        {
            for (int i = 0; i < length; i++)
                if (value.Equals(GetRef<T>(i))) return true;
            return false;
        }

        public bool TryAddConcurrent<T>(ref T obj, int capacity) where T : unmanaged
        {
            int startIndex;
            do
            {
                startIndex = length;
                if (IsFull(capacity))
                    return false;
            } while (startIndex != Interlocked.CompareExchange(ref length, startIndex + 1, startIndex));
            GetRef<T>(startIndex) = obj;
            return true;
        }

        public int TryAddRangeConcurrent<T>(T* values, int valuesLength, int capacity, out int startIndex) where T : unmanaged
        {
            int fillLength;
            do
            {
                startIndex = length;
                if (IsFull(capacity))
                    return 0;
                fillLength = capacity - startIndex;
                if (valuesLength < fillLength)
                    fillLength = valuesLength;
            } while (startIndex != Interlocked.CompareExchange(ref length, startIndex + fillLength, startIndex));
            UnsafeUtility.MemCpy(GetPointer<T>(startIndex), values, sizeof(T) * fillLength);
            return fillLength;
        }

        public bool Remove<T>(ref T item) where T : unmanaged
        {
            for (int i = 0; i < length; i++)
            {
                if (item.Equals(GetRef<T>(i)))
                {
                    RemoveAtSwapBack<T>(i);
                    return true;
                }
            }
            return false;
        }


        public void Dispose(Allocator allocator)
        {
            if (NextNode != IntPtr.Zero)
            {
                Next->Dispose(allocator);
                UnsafeUtility.Free(NextNode.ToPointer(), allocator);
            }
            this = default;
        }
        public NodeConstantCapacity* Next
        {
            get => (NodeConstantCapacity*)NextNode.ToPointer();
            set => NextNode = new IntPtr(value);
        }
        public T* GetPointer<T>(int index) where T : unmanaged => ((T*)Values) + index;
        public T* GetPointer<T>(uint index) where T : unmanaged => ((T*)Values) + index;
        public T* GetPointer<T>(long index) where T : unmanaged => ((T*)Values) + index;
        public T* GetPointer<T>(ulong index) where T : unmanaged => ((T*)Values) + index;
        public static unsafe NodeConstantCapacity* CreatePtr<T>(int capacity, Allocator allocator) where T : unmanaged
        {
            var answer = (NodeConstantCapacity*)UnsafeUtility.Malloc(sizeof(NodeConstantCapacity), 4, allocator);
            *answer = new NodeConstantCapacity(capacity, sizeof(T), allocator);
            return answer;
        }
    }
    public unsafe struct UnrolledLinkedList : IDisposable, IEnumerable<NodeConstantCapacity>
    {
        public NodeConstantCapacity* First;
        public volatile NodeConstantCapacity* LastFull;
        private int capacity;
        public int Capacity => capacity;
        private Allocator allocator;

        public UnrolledLinkedList(int capacity, int elementSize, Allocator allocator)
        {
            this.capacity = capacity;
            this.allocator = allocator;
            LastFull = null;
            First = (NodeConstantCapacity*)UnsafeUtility.Malloc(sizeof(NodeConstantCapacity), 4, allocator);
            *First = new NodeConstantCapacity(capacity, elementSize, allocator);
        }

        public void AddConcurrent<T>(ref T obj) where T : unmanaged
        {
            if (LastFull == null)
            {
                AddToExistingPageConcurrent(First, ref obj);
                return;
            }
            var node = LastFull->Next;
            if (node == null)
                AddNewPageConcurrent(LastFull, ref obj);
            AddToExistingPageConcurrent(node, ref obj);
        }

        private void AddToExistingPageConcurrent<T>(NodeConstantCapacity* node, ref T obj) where T : unmanaged
        {
            while (!node->TryAddConcurrent(ref obj, capacity))
            {
                LastFull = node;
                var next = node->Next;
                if (next == null)
                {
                    AddNewPageConcurrent(node, ref obj);
                    break;
                }
                node = next;
            }
        }

        private void AddNewPageConcurrent<T>(NodeConstantCapacity* node, ref T obj) where T : unmanaged
        {
            var newPage = NodeConstantCapacity.CreatePtr<T>(capacity, allocator);
            newPage->TryAddConcurrent(ref obj, capacity);
            AddNewPageConcurrent(node, newPage);
        }

        private int AddNewPageConcurrent<T>(NodeConstantCapacity* node, T* values, int count) where T : unmanaged
        {
            var newPage = NodeConstantCapacity.CreatePtr<T>(capacity, allocator);
            var consumed = newPage->TryAddRangeConcurrent(values, count, capacity, out _);
            AddNewPageConcurrent(node, newPage);
            return consumed;
        }

        private static void AddNewPageConcurrent(NodeConstantCapacity* node, NodeConstantCapacity* newPage)
        {
            var add = new IntPtr(newPage);
            while (IntPtr.Zero != Interlocked.CompareExchange(ref node->NextNode, add, IntPtr.Zero))
            {
                node = node->Next;
            }
        }

        public unsafe void AddRangeConcurrent<T>(T* values, int count) where T : unmanaged
        {
            NodeConstantCapacity* node = LastFull == null ? First : LastFull;
            while (true)
            {
                while (node->IsFull(capacity))
                {
                    LastFull = node;
                    if (node->NextNode == IntPtr.Zero && ConsumeAndIsEnd(ref values, ref count, AddNewPageConcurrent(node, values, count))) return;
                    node = node->Next;
                }
                if (ConsumeAndIsEnd(ref values, ref count, node->TryAddRangeConcurrent(values, count, capacity, out _))) return;
            }
        }

        private bool ConsumeAndIsEnd<T>(ref T* values, ref int count, int consumed) where T : unmanaged
        {
            count -= consumed;
            values += consumed;
            return count == 0;
        }

        public void Dispose()
        {
            if (First != null)
            {
                First->Dispose(allocator);
                UnsafeUtility.Free(First, allocator);
            }
            this = default;
        }

        public void Clear()
        {
            foreach (ref var node in this)
                node.Clear();
        }

        public bool Contains<T>(ref T item) where T : unmanaged
        {
            foreach (ref var node in this)
                if (node.Contains(ref item))
                    return true;
            return false;
        }

        public NodeEnumerator GetEnumerator() => new NodeEnumerator(this);

        IEnumerator<NodeConstantCapacity> IEnumerable<NodeConstantCapacity>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    public unsafe struct NodeEnumerator : IEnumerator<NodeConstantCapacity>
    {
        private bool isNotFirst;
        private NodeConstantCapacity* node;

        public NodeEnumerator(in UnrolledLinkedList list)
        {
            isNotFirst = false;
            node = list.First;
        }

        public ref NodeConstantCapacity Current => ref *node;
        NodeConstantCapacity IEnumerator<NodeConstantCapacity>.Current => Current;
        object IEnumerator.Current => Current;

        public void Dispose() => this = default;

        public bool MoveNext()
        {
            if (!isNotFirst)
                return isNotFirst = true;
            if (node->NextNode == IntPtr.Zero) return false;
            node = node->Next;
            return true;
        }

        public void Reset() => throw new NotImplementedException();
    }

    public unsafe struct Enumerable<T> : ICollection<T> where T : unmanaged
    {
        private UnrolledLinkedList @this;
        public Enumerable(in UnrolledLinkedList list) => @this = list;

        ref T this[int index]
        {
            get
            {
                if (@this.First == null) throw new NullReferenceException();
                ref var node = ref *@this.First;
                while (true)
                {
                    if (index < node.Length)
                        return ref node.GetRef<T>(index);
                    if (node.NextNode == IntPtr.Zero) throw new IndexOutOfRangeException();
                    index -= node.Length;
                    node = ref *node.Next;
                }
            }
        }

        public int Count
        {
            get
            {
                int count = 0;
                foreach (ref var item in @this)
                    count += item.Length;
                return count;
            }
        }

        public bool IsReadOnly => false;

        public void Add(T item) => @this.AddConcurrent(ref item);

        public void Clear() => @this.Clear();

        public bool Contains(T item) => @this.Contains(ref item);

        public void CopyTo(T[] array, int arrayIndex)
        {
            fixed (T* fixPtr = &array[0])
            {
                var ptr = fixPtr;
                var count = array.Length - arrayIndex;
                foreach (ref var node in @this)
                {
                    var copyLength = count > node.Length ? node.Length : count;
                    UnsafeUtility.MemCpy(ptr, node.Values, sizeof(T) * copyLength);
                    ptr += copyLength;
                    count -= copyLength;
                    if (count == 0) return;
                }
            }
        }

        public Enumerator<T> GetEnumerator() => new Enumerator<T>(@this);

        public bool Remove(T item)
        {
            foreach (ref var node in @this)
                if (node.Remove(ref item))
                    return true;
            return false;
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public static implicit operator Enumerable<T>(in UnrolledLinkedList list) => new Enumerable<T>(list);
        public static implicit operator UnrolledLinkedList(in Enumerable<T> list) => list.@this;
    }

    public unsafe struct Enumerator<T> : IEnumerator<T> where T : unmanaged
    {
        private int index;
        private NodeConstantCapacity Node;
        public Enumerator(in UnrolledLinkedList parent) => (index, Node) = (-1, parent.First == null ? default : *parent.First);
        public ref T Current => ref Node.GetRef<T>(index);
        T IEnumerator<T>.Current => Current;
        object IEnumerator.Current => Current;

        public void Dispose() => this = default;

        public bool MoveNext()
        {
            if (Node.Length == ++index)
            {
                var ptr = Node.Next;
                if (ptr == null) return false;
                Node = *ptr;
                index = 0;
            }
            return true;
        }

        public void Reset() => throw new NotImplementedException();
    }
}