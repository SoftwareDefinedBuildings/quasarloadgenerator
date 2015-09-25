quasarloadgenerator
===================
The code in this repository generates a load for QUASAR in order to measure its performance.

The program can be run in three modes: "Insert", "Query", and "Query & Verify". The command line arguments for these modes are "-i", "-q", and "-v", respectively. Any command line arguments after these specify the UUIDs of the streams to use. Using the command line argument "-p" will verify in "print all" mode.

In "Insert" mode, the program pushes data to a database as quickly as possible. The exact data that gets published is determined by the contents of hte configuration file, which allows one to specify the UUIDs of the streams to insert, the time of the first point, the time between points, the number of points to insert, the number of TCP connections to use, the seed to use to generate random numbers, etc. The configuration file also allows one to specify MAX\_TIME\_RANDOM\_OFFSET, which is the maximum random offset that could be added to each timestamp. This can be used to create unequal, random spacing between points.

In "Query" mode, the program makes queries for data as quickly as possible. The manner in which the data is queried is determined by the same constants listed above.

"Query & Verify" mode is the same as "Query" mode except that the program sacrifices performance in order to verify that the data received matches what would be sent for the same times in "Insert" mode. This can be used to help verify that the data received when querying the database does indeed match the data that was inserted.
