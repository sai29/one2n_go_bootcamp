# 🧩 MongoDB Oplog → SQL Parser

## 📖 Overview

This project converts **MongoDB oplog entries** (`insert`, `update`, `delete`) into equivalent **SQL statements** that can be executed on a relational database (like PostgreSQL).  

It enables live or batch data migration by continuously parsing MongoDB oplogs and mirroring changes on SQL tables. Use your oplog live stream or an oplog json file as input and write to a SQL file or directly to Postgres.

---

## 🧱 Problem Statement

When migrating from MongoDB to a relational database, simply exporting data isn’t enough — changes keep happening.  
MongoDB’s **Oplog** records all writes (`insert`, `update`, `delete`).  

This tool reads those oplogs and **translates them into SQL** to maintain an up-to-date mirror in SQL databases.

---

## ✅ Features Completed (Stories 1–9)

| Story | Description | Status |
|-------|--------------|--------|
| 1 | Insert parsing → converts MongoDB inserts to SQL `INSERT INTO` | ✅ |
| 2 | Update parsing → handles `$v`, `$diff` (`u`, `d`) for field updates/unsets | ✅ |
| 3 | Delete parsing → generates SQL `DELETE FROM ... WHERE ...` | ✅ |
| 4 | Create table → generates `CREATE SCHEMA`, `CREATE TABLE`, and `INSERT` | ✅ |
| 5 | Multiple inserts → prevents duplicate `CREATE TABLE` for same collection | ✅ |
| 6 | Alter table → detects new fields and generates `ALTER TABLE ADD` | ✅ |
| 7 | Nested documents → handles nested objects/arrays as separate tables | ✅ |
| 8 | File I/O → reads oplogs from JSON file, writes SQL to output file | ✅ |
| 9 | MongoDB streaming → reads oplogs directly from MongoDB and applies to PostgreSQL | ✅ |
| 10 | Bookmarking Support → Parser can keep track of the last oplog, resume processing from the correct point after a restart.| ✅ |
| 11 | Distributed Execution → Distribute the parser's execution across multiple machines to achieve higher performance and scalability.  | ✅ |


---

## 🧠 Architecture (Courtesy of One2N)

<img src="./assets/oplog2sql-multi-goroutine.png" width="50%">

