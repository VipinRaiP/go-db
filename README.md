# GoDB

GoDB is a lightweight, ACID-compliant, disk-based relational database management system written purely in Go. This project is built as an educational exercise to demonstrate and explore the core underlying components of modern relational database engines.

## 🏗 Architecture & Features

GoDB's architecture closely mimics the layered design of monolithic database systems like PostgreSQL or SQLite. 

### 1. Storage Engine (`pkg/storage`)
* **Pager**: The lowest level disk manager. It abstracts physical I/O by splitting the main database file into fixed 4KB blocks (Pages) and provides indexed `ReadPage` and `WritePage` interfaces.
* **WAL (Write-Ahead Log)**: Ensures durability and crash recovery. Every modifying query is synchronously persisted to a sequential log append-only file before it is written to the main B-Tree or returned as "successful" to the user.

### 2. Memory Management (`pkg/buffer`)
* **Buffer Pool**: An in-memory cache system that sits between the database logic and the disk. It reduces slow disk I/O by keeping frequently accessed 4KB pages in RAM.
* **LRU Eviction**: When memory limits are reached, the Buffer Pool utilizes a Least Recently Used (LRU) algorithm to safely evict and flush inactive pages back to disk.

### 3. Execution & Indexing (`pkg/index`, `pkg/sql`)
* **B+ Tree Index**: The primary data structure for row storage. It allows $O(\log n)$ logarithmic time complexity for `INSERT` and `SELECT` query lookups. Records are stored securely inside linked Leaf Nodes, while Internal Nodes provide navigation.
* **SQL Executor & Parser**: Handles translating string queries into physical database operations.

## 🚀 Getting Started

*(Instructions will be added here on how to build and run the REPL once complete)*
