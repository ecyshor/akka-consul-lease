## Akka lease implementation which uses consul for coordination

### Usage

### Implementation

The lease is implemented according to the [Akka documentation](https://doc.akka.io/docs/akka/current/coordination.html).

To support leases consul has the notion of [sessions](https://www.consul.io/api/session.html) and [locks](https://www.consul.io/docs/commands/lock.html). You create a session and using the session you acquire a lock 
in the kv store. This behaviour is describe in the consul example of implementation [leader election](https://learn.hashicorp.com/consul/developer-configuration/elections)

Because consul has the notion of ttl for sessions (must be renewed before the ttl expires or the locks are released) and lock delay in the session (if the session is invalidated then the locks will be release only after the lock delay) the heartbeat intervals are not precise and could variate. 

#### Notes

Might introduce cats in the future to simplify the code

### Configuration

Mapping between the akka lease config and consul config:

- heartbeat-timeout
    - akka-meaning: if the node that acquired the leases crashes, how long should the lease be held before another owner can get it
    - consul mapping: session ttl
    - notice: the session ttl is renewed every heartbeat-interval so the timeout varies between (the heartbeat timeout minus the heartbeat interval and the value that consul will use to actually invalidate the session as according to the docs `When locks are forcibly expired, such as when following the leader election pattern in an application, sessions may not be reaped for up to double this TTL, so long TTL values (> 1 hour) should be avoided.`)
   
- heartbeat-timeout
    - interval for communicating with the third party to confirm the lease is still held
    - consul mapping: renew session

