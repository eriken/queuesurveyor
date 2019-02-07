# queuesurveyor

In casees where redis lists are used as a queue, this library adds
retry functionlity.

The library relies on the poppush functionality of redis. As the
consumer pops a record of the list, it is transactionally also
pushed to a progress list, refered to as the "archive". If these
records are kept in the archive list longer than a certain amount
of time the records are pushed to the list they were originally
popped from, refered to as the "current" list.

The library holds no state on each record so amount of retries
are indefinite.