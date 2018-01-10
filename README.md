# rabedos
Reliable Automatic Brisk Extensible Distributed Object Storage, including block device, object storage and file system.

RabeDos consists of three types of components, DataNode, MetaNode and Controller mainly. DataNodes take charge of data storage, maintain storage resource. MetaNodes is responsible for meta data management, including data persistence, update, and the like. For controllers, they are the soul of entire system. Including topological structure of datanode and metanode cluster, replication group mapping, metanode group mapping, failure recovery, exception handle mechanism, automatic scale out and others, are all or partially depended of controllers.

design objectives
1. Storage scale can easily scale from PB level to EB level.
2. Adaptive to large/tiny object storage, and ensure stable and efficient IO performance.
3. Distinguishing hot and cold data processing to compress storage space.
4. Brife cluster rescale and maintenance, and real automatic and reliable elastic cloud storage.
5. Fully self-recovery mechanism and strongly consistent data protection.
6. Open and flexible standard access interface supports a variety of application scenarios.
7. Visual operation and maintenance monitoring platform.
