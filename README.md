# gibd
## Introduction
This project is just for learning golang and learning how to parse innodb blocks. just finished dump dictionary header block and list system space. For datatype, I just finished Integer and varchar TransactionId, RollPointer implementation.

## Usage
```
go run main.go -s ibdata1 -m system-spaces
print record header 
go run main.go -s dba_user2.ibd -p 3 -m page-dump
```