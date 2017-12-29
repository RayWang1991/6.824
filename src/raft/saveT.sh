#!/bin/bash
echo "Start test";
echo "" > saveLog1.out;
go test -run=TestBackup >> saveLog1.out;
