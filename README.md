[![Go Report Card](https://goreportcard.com/badge/github.com/anayrat/pgcheetah)](https://goreportcard.com/report/github.com/anayrat/pgcheetah) [![License](https://img.shields.io/badge/License-PostgreSQL-blue.svg)](https://github.com/anayrat/pgcheetah/blob/master/LICENSE)

# pgcheetah

pgcheetah is a mix between pgbench and a tool to replay statements. It takes
a sample dataset to replay it *randomly*.

His goal is to reproduce a *real* workload from a sample of statements.
It also collects wait events statistics (the reason why it has been developped).

Thanks to Go and goroutines, pgcheetah is able to handle several hundred of clients and a quite high throughput in TPS.
It has been successfully tested up to thousand clients and 200K TPS with a sample collected by [pg_sampletolog
](https://github.com/anayrat/pg_sampletolog). Note that it may necessary to clean collected sample to generate a clean dataset.
For example, by logging to CSV, import in a postgres database, clean "noise" queries and use a window function to reorder
transactions.

You must provide a sample dataset with ordered transactions. pgcheetah will "parse" the dataset to idenfify transactions.
Then, it will start clients which choose a random transaction and replay it in the right order.

There are many options to control how to replay the workload:

  * Number of clients
  * Expected *transaction per seconds* or delay betweend each transaction
  * Lenght of the test
  * User think time

User think time is useful to reproduce *idle in transaction* sessions. Actually you must provide a min and a max
in milliseconds and each client will draw a random number in this range (uniform distribution).

Even if nothing forbid to replay write query, in real life there is few chance it will work. We could be facing
problems such as unique and foreign key constraints.

If you have an error during parsing phase, you can enable debug option. This will display each parsed line and can be helpful to
clean the dataset.

## Options

Usage of ./pgcheetah:

  * clients:
    	number of client (default 100)
  * constr:
    	pg connstring (default "user=postgres dbname=postgres")
  * debug:
    	debug mode
  * delaystart:
    	spread clients start among seconds
  * delayxact:
    	millisecond between each transaction (default 5)
  * duration:
    	Test duration in seconds
  * interval:
    	Interval stats report (default 1 second)
  * netpprof:
    	enable internal pprof web server
  * queryfile:
    	path to file containing queries to play
  * slowstartfactor:
    	Factor to control how fast the delay between transaction will be changed (default 1.6)
  * thinktimemax:
    	millisecond thinktime (default 5)
  * thinktimemin:
    	millisecond thinktime (default 5)
  * tps:
    	expected tps


## Example

pgcheetah will reports metrics on stdout output (each *interval* seconds) and collects [wait events](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS-SETUP) statistics every 500ms:

```
./pgcheetah -clients 1000 -tps 200000 -constr 'user=user1 dbname=db1 host=pg.local' -thinktimemin 0 -thinktimemax 0 -delaystart 30 -delayxact 30 -queryfile play-20k.sql -duration 40 -interval 5 -slowstartfactor 2
2019/04/26 15:37:20 Start parsing
2019/04/26 15:37:20 Parsing done, start workers. Transactions processed: 19960
2019/04/26 15:37:50 All workers launched
2019/04/26 15:37:51 TPS: 302450 QPS: 338850 Xact: 30245 Queries: 33885 Delay: 30ms Remaining: 39s
2019/04/26 15:37:56 TPS: 61580 QPS: 69070 Xact: 264135 Queries: 296984 Delay: 15.675ms Remaining: 34s
2019/04/26 15:38:01 TPS: 96660 QPS: 108750 Xact: 660486 Queries: 742695 Delay: 9.703ms Remaining: 29s
2019/04/26 15:38:06 TPS: 131960 QPS: 147740 Xact: 1239210 Queries: 1393970 Delay: 6.404ms Remaining: 24s
2019/04/26 15:38:11 TPS: 183880 QPS: 206850 Xact: 2047366 Queries: 2304641 Delay: 4.377ms Remaining: 19s
2019/04/26 15:38:16 TPS: 202420 QPS: 228370 Xact: 3040840 Queries: 3422127 Delay: 3.816ms Remaining: 14s
2019/04/26 15:38:21 TPS: 208660 QPS: 243780 Xact: 4043817 Queries: 4550454 Delay: 3.943ms Remaining: 9s
2019/04/26 15:38:26 TPS: 200270 QPS: 224500 Xact: 5047839 Queries: 5680047 Delay: 3.891ms Remaining: 4s
2019/04/26 15:38:30 Test finished, stop clients
2019/04/26 15:38:30 End test - Clients: 1000 - Elapsed: 40.090753907s - Average TPS: 145918 - Average QPS: 164192
2019/04/26 15:38:30 Wait_event count:
LWLockTranche-lock_manager      - 22
```

Here is a test with 1000 clients and 200KTPS expected.

First step is parsing, then clients are started among *delaystart* seconds to avoid a spike when starting.

When all clients are started, rate limiting begin and it changes the delay between each transaction to reach expected tps.
First line is generally wrong because clients had already performed activity before rate limiter start. It is possible
to control reduce clients activity before limiter by increasing delayxact.
*slowstartfactor* allow to control the aggressiveness of how pgcheetah try to reach expected tps.
Here, it took ~15s to reach 200KTPS.

By the end of the test (*duration* setting) or if you hit ctrl-c, all the clients will be stopped and wait event are reported.

In this example Average TPS is less than expected TPS, it is due to short test and slowstart.

## Notice

Please note, it is a quick and dirty tool. For example, instead of using a real parser, it only identify few statements type
(Begin, commit etc) by using regexp. It may not works with your queries and require few changes.
