

test


Each "Passed" line contains five numbers; 
1. these are the time that the test took in seconds,
2. the number of Raft peers (usually 3 or 5), 
3. the number of RPCs sent during the test,
4. the total number of bytes in the RPC messages, 
5. and the number of log entries that Raft reports were committed.


Test (2C): Figure 8 (unreliable) ...
2021/06/17 20:26:05 3: log map[1:7634 2:4600]; server map[1:7634 2:4600 3:1602 4:3069 5:101 6:5537 7:3018 8:8050 9:4907 10:7635 11:6752 12:181 13:2531 14:7974 15:6708 16:5866 17:9689 18:2133 19:2358 20:6640 21:9781 22:7711 23:8812 24:5815]
2021/06/17 20:26:05 apply error: commit index=3 server=3 4600 != server=4 1602