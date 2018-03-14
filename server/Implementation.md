# Tasks life-cycle

* the `active` set is containing all the tasks currently active (not yet stopped)
* there are 2 types of queue:
  * _service queue_ (`queued:<service>`) - containing tasks waiting for resource allocation
  * _work queue_ (`work`) - the task currently in process (except running tasks that have their own life-cycle as long as they get updated)

* each task is queued on its _service queue_ at creation

* depending on available resources, tasks from _service queues_ considered for allocation (depending on priority and dependencies) are pushed on `queue` and removed from their _service queue_

* all the tasks on _work queue_ are considered for _advancement_ depending on their status:
  * `queue` - if resource can be allocated, task moves to `allocated`, otherwise, task is sent back to its _service queue_. If task depends on non stopped task (should not happen), it is also pushed back to its _service queue_. If task depends on stopped but not successful task, it is terminated with dependency failure.
  * `allocated` - task is launched, status is changed to `running`, task is removed from the queue
  * `running` - task status is checked by inspection on running resource, if task is still running, task is removed from the queue, otherwise changed to `terminating` status
  * `terminating` - task is terminated, removed from the queue, removed from `active` set.

* running tasks have a `queue:<task_id>` entry in redis with TTL. When the TTL expires, the task is put back on the _work queue_. TTL is renewed when beat is received.

When a worker is started:
* all active tasks are pushed back on _work_ or _service queues_ depending on their status.
