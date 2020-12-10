Clickhouse Database
===================

The mpi lstat collector pipeline creates protocol buffer encoded data files. The pipeline loads these into a cickhouse database using the clickhouse-client command line. You will need to have this available on all your compute nodes. The simplest way to do this is to just install the clickhouse client package as described on the clickhouse web page.

You will need to install a clickhouse server as well. A machine with a lot of cores, RAM and networking bandwidth is recommended. We have tried 2 different types of hardware.

1) An old Dell C6145 with 4 AMD 16-core cpus and 512GB of RAM. We also put in 2 Samsung EVO SATA SSDs configured as a Raid0 array using MegaCLI to store the clickhouse database files.

2) An even older server with 2 Intel Xeon E5-2640 @ 2.60GHz (total of 16 cores) and 256GB of RAM. We put a single SSD in this server for the clickhouse data.
