# gibd
## Introduction
This project is just for learning golang and learning how to parse innodb blocks . For datatype, I just finished Integer and varchar, TransactionId, RollPointer implementation.

## Usage
```

go run main.go -s ibdata1 -m system-spaces

go run main.go -s dba_user2.ibd -p 3 -m page-dump
```
##  TODO
```
parse undo block
read table information and parse record by parsing config file when there is no dictionary  info 
print all rows for user tables in ibd file
```
