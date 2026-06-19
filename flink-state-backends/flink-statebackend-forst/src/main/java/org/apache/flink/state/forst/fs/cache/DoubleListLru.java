/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs.cache;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A double link LRU (Least Recently Used) cache implementation. This cache maintains two linked
 * lists to manage the cache entries. The first list contains the most recently used entries, and
 * the second list contains the less recently used entries. The cache also maintains a middle
 * pointer to efficiently manage the entries. No thread-safe guarantees are provided.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
public abstract class DoubleListLru<K, V> implements Iterable<Tuple2<K, V>> {

    class Node {
        K key;
        V value;
        Node prev;
        Node next;
        boolean isBeforeMiddle;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
            this.isBeforeMiddle = false;
        }
    }

    private Node head;
    private Node tail;
    private Node middle;
    private int size;
    private int secondSize;
    private final HashMap<K, Node> map;

    public DoubleListLru() {
        this.head = null;
        this.tail = null;
        this.middle = null;
        this.size = 0;
        this.map = new HashMap<>();
    }

    // -------------------
    // Hook methods
    // -------------------

    /**
     * Checks if it is safe to add a value to the first list. Will be called before adding a value
     * to the first list.
     *
     * @param value the value to be added to the first list
     * @return true if it is safe to add the value to the first list, false otherwise.
     */
    abstract boolean isSafeToAddFirst(V value);

    /**
     * Hook method called when a new node is created.
     *
     * @param value the value of the new node
     * @param n the new node
     */
    abstract void newNodeCreated(V value, Node n);

    /**
     * Hook method called when a value is added to the first list.
     *
     * @param value the value added to the first list
     */
    abstract void addedToFirst(V value);

    /**
     * Hook method called when a value is added to the second list.
     *
     * @param value the value added to the second list
     */
    abstract void addedToSecond(V value);

    /**
     * Hook method called when a value is removed from the first list.
     *
     * @param value the value removed from the first list
     */
    abstract void removedFromFirst(V value);

    /**
     * Hook method called when a value is removed from the second list.
     *
     * @param value the value removed from the second list
     */
    abstract void removedFromSecond(V value);

    /**
     * Hook method called when a value is moved to the first list.
     *
     * @param value the value moved to the first list
     */
    abstract void movedToFirst(V value);

    /**
     * Hook method called when a value is moved to the second list.
     *
     * @param value the value moved to the second list
     */
    abstract void movedToSecond(V value);

    /**
     * Hook method called when a node is accessed in the second list.
     *
     * @param value the value of the accessed node
     * @return true if the node should be promoted to the first list, false otherwise
     */
    abstract boolean nodeAccessedAtSecond(V value);

    /**
     * Hook method called when a value is promoted to the first list.
     *
     * @param value the promoted value
     */
    abstract void promotedToFirst(V value);

    /**
     * Adds a new entry to the front of the cache.
     *
     * @param key the key of the entry
     * @param value the value of the entry
     */
    public void addFirst(K key, V value) {
        if (!isSafeToAddFirst(value)) {
            addSecond(key, value);
            return;
        }
        Node newNode = new Node(key, value);
        newNodeCreated(value, newNode);
        map.put(key, newNode);
        if (head == null) {
            head = tail = newNode;
        } else {
            newNode.next = head;
            head.prev = newNode;
            head = newNode;
        }
        newNode.isBeforeMiddle = true;
        size++;
        addedToFirst(value);
    }

    /** Moves the middle pointer back by one position. */
    public void moveMiddleBack() {
        if (middle != null) {
            middle.isBeforeMiddle = true;
            V theValue = middle.value;
            middle = middle.next;
            secondSize--;
            movedToFirst(theValue);
        }
    }

    /** Moves the middle pointer forward by one position. */
    public void moveMiddleFront() {
        if (middle != null && middle.prev != null) {
            middle = middle.prev;
            middle.isBeforeMiddle = false;
            secondSize++;
            movedToSecond(middle.value);
        } else if (middle == null && size > 0) {
            middle = tail;
            middle.isBeforeMiddle = false;
            secondSize++;
            movedToSecond(middle.value);
        }
    }

    /**
     * Inserts a new entry at the middle of the cache.
     *
     * @param key the key of the entry
     * @param value the value of the entry
     */
    public void addSecond(K key, V value) {
        Node newNode = new Node(key, value);
        newNodeCreated(value, newNode);
        map.put(key, newNode);
        if (head == null) {
            head = tail = middle = newNode;
        } else if (middle == null) {
            newNode.prev = tail;
            tail.next = newNode;
            tail = newNode;
            middle = newNode;
        } else {
            newNode.next = middle;
            newNode.prev = middle.prev;
            if (middle.prev != null) {
                middle.prev.next = newNode;
            } else {
                // head == middle
                head = newNode;
            }
            middle.prev = newNode;
            middle = newNode;
        }
        newNode.isBeforeMiddle = false;
        secondSize++;
        size++;
        addedToSecond(value);
    }

    /**
     * Returns the value of the middle entry in the cache.
     *
     * @return the value of the middle entry, or null if the cache is empty
     */
    @VisibleForTesting
    V getMiddle() {
        return middle != null ? middle.value : null;
    }

    /**
     * Retrieves the value associated with the specified key. Optionally affects the order of the
     * entries in the cache.
     *
     * @param key the key of the entry
     * @param affectOrder true if the order of the entries should be affected, false otherwise
     * @return the value associated with the key, or null if the key is not found
     */
    public V get(K key, boolean affectOrder) {
        Node node = map.get(key);
        if (node == null) {
            return null;
        }
        if (!affectOrder) {
            return node.value;
        }
        accessNode(node);
        return node.value;
    }

    /**
     * Removes the entry associated with the specified key from the cache.
     *
     * @param key the key of the entry to be removed
     * @return the value of the removed entry, or null if the key is not found
     */
    public V remove(K key) {
        Node node = map.get(key);
        if (node == null) {
            return null;
        }
        if (node == head || node == tail) {
            if (node == head) {
                head = node.next;
                if (head != null) {
                    head.prev = null;
                }
            }
            if (node == tail) {
                tail = node.prev;
                if (tail != null) {
                    tail.next = null;
                }
            }
        } else {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
        if (node == middle) {
            middle = node.next;
        }
        map.remove(key);
        size--;
        if (node.isBeforeMiddle) {
            removedFromFirst(node.value);
        } else {
            secondSize--;
            removedFromSecond(node.value);
        }
        return node.value;
    }

    void accessNode(Node node) {
        if (node.isBeforeMiddle) {
            moveToFront(node);
        } else {
            moveToMiddle(node);
            if (nodeAccessedAtSecond(node.value) && isSafeToAddFirst(node.value)) {
                moveMiddleBack();
                moveToFront(node);
                promotedToFirst(node.value);
            }
        }
    }

    /**
     * Moves the specified node to the front of the cache.
     *
     * @param node the node to be moved to the front
     */
    private void moveToFront(Node node) {
        assert node.isBeforeMiddle;
        if (node == head) {
            return;
        }
        if (node == tail) {
            tail = node.prev;
            tail.next = null;
        } else {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
        node.prev = null;
        node.next = head;
        head.prev = node;
        head = node;
    }

    /**
     * Moves the specified node to the middle of the cache.
     *
     * @param node the node to be moved to the middle
     */
    private void moveToMiddle(Node node) {
        assert !node.isBeforeMiddle;
        if (node == middle) {
            return;
        }
        if (node == tail) {
            tail = node.prev;
            tail.next = null;
        } else {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
        node.next = middle;
        node.prev = middle.prev;
        if (middle.prev != null) {
            middle.prev.next = node;
        } else {
            // head == middle
            head = node;
        }
        middle.prev = node;
        middle = node;
    }

    /**
     * Returns the number of entries in the cache.
     *
     * @return the number of entries in the cache
     */
    public int size() {
        return size;
    }

    public int getSecondSize() {
        return secondSize;
    }

    /**
     * Checks if the cache is empty.
     *
     * @return true if the cache is empty, false otherwise
     */
    public boolean isEmpty() {
        return size == 0;
    }

    // ----------------------
    // Iterators
    // ----------------------

    @Override
    public Iterator<Tuple2<K, V>> iterator() {
        return new LruIterator();
    }

    public Iterator<Tuple2<K, V>> descendingIterator() {
        return new DecendingLruIterator();
    }

    private class LruIterator implements Iterator<Tuple2<K, V>> {
        private Node current = head;

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Tuple2<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple2<K, V> entry = Tuple2.of(current.key, current.value);
            current = current.next;
            return entry;
        }
    }

    private class DecendingLruIterator implements Iterator<Tuple2<K, V>> {
        private Node current = tail;

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Tuple2<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple2<K, V> entry = Tuple2.of(current.key, current.value);
            current = current.prev;
            return entry;
        }
    }
}
