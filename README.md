# Hadoop Jobs Profiler

This Scala program allows for profiling Hadoop jobs, extracting data from properly
parsed execution logs.
Further, it is possible to exploit the obtained profiles to check if sessions perform
as expected.

Code in this repository is licensed under the
[Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Usage Note

Alongside the required files output by [LogParser](https://github.com/deib-polimi/LogParser),
you can provide also `appId.txt` and `appUsers.txt`.

* `appId.txt`: a text file with tab-separated lines reporting the class name as
               first field, followed by a list of job IDs.
* `appUsers.txt`: a text file with tab-separated lines reporting the class name
                  and its number of concurrent users.
