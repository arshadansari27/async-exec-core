Command Pattern / Observer Pattern based backend asynchronous task execution framework. 

Ability to run async within the same process or as a seperate daemon service
Currently on supports Redis.

ROAD MAP:
    * Allow multiple listeners and corresponding handlers with respective queues
      for request response
    * Use decorator to attach user handlers to the listeners
    * Use queue names to seperate listeners (although there seems to be no
      reason to have seperate queues except for priority, but looking for a
      better way to achieve that)
