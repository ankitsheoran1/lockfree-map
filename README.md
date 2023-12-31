# lockfree-map

Here map is implemented as a set of buckets , each buckets in sorted order , so if a key has same hash then it would increase sorted list length for that bucket and adverse effect time complexity 
The cleanup function is using an epoch-based synchronization mechanism to ensure safe cleanup and deletion of resources.
Epoch-based Synchronization: The cleanup function sets up an epoch-based synchronization mechanism to coordinate the cleanup process with other threads or handles that might be accessing the shared resources concurrently.

For ref follow this Doc https://hal.science/hal-01207881/document
and also for linked list which is used in each bucket follow this research paper https://www.cl.cam.ac.uk/research/srg/netos/papers/2001-caslists.pdf

Also this is highly motivated by Aditya Saligrama' s Implementation so pls checkout his github repo also https://github.com/saligrama