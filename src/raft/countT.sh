#!/bin/bash
echo "Start test";
echo "" > tmp.out;
for i in {1..100};
do echo "NO."$i " testing";
   go test -run=TestUnreliableAgree >> tmp.out;
done;
echo "passed test num:";
fgrep "ok" ./tmp.out | wc -l;
echo "finish test";
#rm ./tmp.out
