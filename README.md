# queuesurveyor

In case a service relies on consuming entries in redis list as queue,
this library adds retry functionlity to these scenarios. This library
relies on the poppush functionality of redis. As the consumer pops a
record of the list, it is transactionally also pushed to a progress
list, refered to as the "archive". If these records are kept in the
archive list longer than a certain amount of time the records are
pushed to the list they were originally pop from, refered to as the
"current" list.