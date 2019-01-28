# Storage_And_Buffer_Manager
The program manages a buffer of blocks in memory using suitable block replacement strategy.

- The application implements a storage manager that allows read/write of blocks to and from a disk.
- It maintains these blocks using a buffer manager which keeps a fixed number of pages in the buffer.
- The buffer pool uses a page replacement strategy (either FIFO or LRU) which is determined when the it is initialized.
- Technologies used: C/C++
