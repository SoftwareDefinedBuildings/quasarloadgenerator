quasarloadgenerator
===================
The code in this repository generates a load for QUASAR in order to measure its performance.

The program can be run in three modes: "Insert", "Query", and "Query & Verify". The command line arguments for these modes are "-i", "-q", and "-v", respectively. Any command line arguments after these specify the UUIDs of the streams to use.

In "Insert" mode, the program pushes data to a database as quickly as possible. The exact data that gets published is determined by constants in the program, which are:
TOTAL_RECORDS - The total number of points to send.
TCP_CONNECTIONS - The number of TCP connections to use.
POINTS_PER_MESSAGE = - The number of points to send per message.
NANOS_BETWEEN_POINTS - The number of nanoseconds between points.
DB_ADDR - The URI of the database (e.g. "localhost:4410")
NUM_STREAMS - The number of streams to use.
FIRST_TIME - The time of the first point to send
Every second, it prints out how many points it has sent in the past second, and how many responses it has received for those points.

In "Query" mode, the program makes queries for data as quickly as possible. The manner in which the data is queried is determined by the same constants listed above.

"Query & Verify" mode is the same as "Query" mode except that the program sacrifices performance in order to verify that the data received matches what would be sent for the same times in "Insert" mode. The only thing unchecked is that the points are at the same times as would normally be sent; only the values are checked. The total number of points verified to be correct is verified at the end, so that it is possible to check that the number of points queried matches what is expected.