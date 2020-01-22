## Akka lease implementation which uses consul for coordination

### Usage

Default config

```hocon
my-lease {
  lease-class = "com.github.ecyshor.akka.lease.consul.ConsulLease"
  heartbeat-timeout = 100s
  heartbeat-interval = 5s
  lease-operation-timeout = 1s
  kv-prefix = akka/leases
  consul {
   scheme = "http"
   host = "localhost"
   port = 8500
   timeout = 5 seconds
  }
}
```

```scala
import akka.actor.ActorSystem
import akka.coordination.lease.scaladsl.LeaseProvider
val system = ???
LeaseProvider(system).getLease("my-lease-name", "my-lease", "owner")
```

Also check [LeaseSpec](src/test/scala/com/github/ecyshor/akka/lease/consul/LeaseSpec.scala)

### Implementation

The lease is implemented according to the [Akka documentation](https://doc.akka.io/docs/akka/current/coordination.html).

To support leases consul has the notion of [sessions](https://www.consul.io/api/session.html) and [locks](https://www.consul.io/docs/commands/lock.html). You create a session and using the session you acquire a lock 
in the kv store. This behaviour is describe in the consul example of implementation [leader election](https://learn.hashicorp.com/consul/developer-configuration/elections)

Because consul has the notion of ttl for sessions (must be renewed before the ttl expires or the locks are released) and lock delay in the session (if the session is invalidated then the locks will be release only after the lock delay) the heartbeat intervals are not precise and could variate. 

#### Notes

Might introduce cats in the future to simplify the code

To implement it there are two concepts in consul which have to be understood.

##### Sessions
To create a lease we must obtain a lock in the consul kv store. Locks can only be acquired by sessions.
The session define the TTL for all the locks acquired by that session. We renew the session every TTL/3 to get a new ttl life.
We manage the session using a single actor in the actor system so that we can share it between leases and to not create one session per lock/lease.
Sessions also define a lock-delay which ensure the lock cannot be acquired immediately after the ttl expires, as a safety measure.  
##### Locks
Locks are represented in consul by a regular kv entry. Using the session we acquire the lock. 
The blocking query mechanism to get updates about a key is not used currently but we read the key value during each heartbeat and check the
current session owner.  

Resources here:
- https://www.consul.io/docs/internals/sessions.html
- https://learn.hashicorp.com/consul/developer-configuration/elections

### Configuration

| Key        | Default           | Notes  |  Meaning |
| ------------- |:-------------:| -----:| -----:|
| lock-delay    | heartbeat-interval * 2 | The delay ensure that any lease will determine that it has lost the lock before another owner can acquire it. Can be set to 0 to disable it but it's recommended to not change this value, and in turn make sure the heartbeat-interval is under 30s to not exceed the lock delay max value   |As per consul notes `This is a time duration, between 0 and 60 seconds. When a session invalidation takes place, Consul prevents any of the previously held locks from being re-acquired for the lock-delay interval; this is a safeguard inspired by Google's Chubby. The purpose of this delay is to allow the potentially still live leader to detect the invalidation and stop processing requests that may lead to inconsistent state. While not a bulletproof method, it does avoid the need to introduce sleep states into application logic and can help mitigate many issues. While the default is to use a 15 second delay, clients are able to disable this mechanism by providing a zero delay value.` |
| session-ttl   | heartbeat-timeout      | When this expires all the locks are released but cannot be acquire until after the lock-delay expires | As per consul notes `When creating a session, a TTL can be specified. If the TTL interval expires without being renewed, the session has expired and an invalidation is triggered. This type of failure detector is also known as a heartbeat failure detector. It is less scalable than the gossip based failure detector as it places an increased burden on the servers but may be applicable in some cases. The contract of a TTL is that it represents a lower bound for invalidation; that is, Consul will not expire the session before the TTL is reached, but it is allowed to delay the expiration past the TTL. The TTL is renewed on session creation, on session renew, and on leader failover. When a TTL is being used, clients should be aware of clock skew issues: namely, time may not progress at the same rate on the client as on the Consul servers. It is best to set conservative TTL values and to renew in advance of the TTL to account for network delay and time skew.` |
| session-renew-interval | heartbeat-timeout / 3      |    The interval used to renew the session ttl so it does not expire. We try to consider the session valid until we still can renew it safely | |
| consul.scheme | http |     | |
| consul.host | localhost |  | |
| consul.port | 8500 |  | |
| consul.timeout | 5 seconds |     | |
| kv-prefix | akka/leases/ |   The kv prefix to use to create the lock path. The prefix is suffixed by the lease name  | Warning: Changing the prefix basically creates a new lease |
| session-actor-name | akka-lease-consul-session-actor |   The name of the actor used to handle consul sessions. Ensures only one with the same name exists on the same actor system  | Can be used to have multiple sessions on the same node if needed but it doesn't make sense in most cases as the node is the owner and we want to share the same session |

