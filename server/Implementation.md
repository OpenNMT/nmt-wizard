# Tasks life-cycle

* the `active` set is containing all the tasks currently active (not yet stopped)
* there are 2 types of queue:
  * _service queue_ (`queued:<service>`) - containing tasks waiting for resource allocation
  * _work queue_ (`work`) - the task currently in process (except running tasks that have their own life-cycle as long as they get updated)

* each task is queued on its _service queue_ at creation

* depending on available resources, tasks from _service queues_ considered for allocation (depending on priority and dependencies) are pushed on `queue` and removed from their _service queue_

* all the tasks on _work queue_ are considered for _advancement_ depending on their status:
  * `queue` - if resource can be allocated or partially allocated, task moves to `allocated` or `allocating` (depending on availability of gpu), otherwise, task is sent back to its _service queue_. If task depends on non stopped task (should not happen), it is also pushed back to its _service queue_. If task depends on stopped but not successful task, it is terminated with dependency failure.
  * `allocating` - task has been been attributed one resource on a server, once another resource on the same server will be available it will be added to the allocation pool till it reaches full allocation 
  * `allocated` - task is launched, status is changed to `running`, task is removed from the queue. If the task cannot be launched (for any reason), the corresponding resource is flagged as busy, the task is unallocated and goes back to the service queue
  * `running` - task status is checked by inspection on running resource, if task is still running, task is removed from the queue, otherwise changed to `terminating` status
  * `terminating` - task is terminated, removed from the queue, removed from `active` set.

* running tasks have a `queue:<task_id>` entry in redis with TTL. When the TTL expires, the task is put back on the _work queue_. TTL is renewed when beat is received.

When a worker is started:
* all active tasks are pushed back on _work_ or _service queues_ depending on their status.

# Resources status

Tasks running on a given resource are listed in corresponding redis list `resource:<service>:<resourceid>`.
If a launch fails (node not reachable, or actual resource not available on the node) - the corresponding resource is put in _quarantine_ for time specified in configuration file. During this time, the resource will not be allocated anymore.

Allocation of resource is made following the rules:
 * a resource cannot be in `reserved` mode or `busy` mode
 * the remaining capacity should be minimal

If a multi-gpu task is launched without all allocated gpus, the corresponding resource is put in reserved mode (`reserved:<service>:<resourceid>=task_id`).
