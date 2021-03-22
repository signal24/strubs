**STRUBS (Striping & Redundancy using Basic Disks)**

STRUBS is a single-host fault-tolerant mass-storage service for providing a scalable storage system for small scale operations.

It was designed as an alternative to RAID after encountering multiple failures of obsolete RAID controllers at a client site.
It has been in production use since 2017, storing 60+ TB (growing daily) of photos and videos across 25 disks of various sizes.

The design was inspired by [Backblaze's Vault architecture](https://www.backblaze.com/blog/vault-cloud-storage-architecture/).
Development would have not been possible were it not for Backblaze's [open sourcing](https://github.com/Backblaze/JavaReedSolomon)
of their Reed-Solomon implementation and [@ronomon's subsequent port](https://github.com/ronomon/reed-solomon) to Node.js.

Prior to deciding to build STRUBS, we tried out [FreeNAS](https://www.freenas.org) but the then-stable version was having major performance and stability issues
on our hardware, plus we didn't want to risk another data incident with a multi-disk filesystem. The benefit of a design like Vault or STRUBS is that one can never
suffer a complete array failure, since the system is really composed of files distributed across independent disks, each with their own independent filesystem.

Ultimately, the goal for STRUBS was to serve as a solid single-host multi-drive storage system that provide automatic provisioning for drive expansion and drive replacement,
embedded SMART monitoring, and seamless automated backup to external media.

[MinIO](https://min.io) is arguably the best alternative for object storage today, but it was still in its infancy at the time STRUBS was created.

The future of STRUBS is still to be decided and this README will be updated as the roadmap unfolds.