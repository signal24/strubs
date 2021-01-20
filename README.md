**STRUBS (Striping & Redundancy using Basic Disks)**

STRUBS is a single-host fault-tolerant mass-storage service for providing a scalable storage system for small scale operations.

It was designed as an alternative to RAID after encountering multiple failures of obsolete RAID controllers at a client site.
It has been in production use to storage 60+ GB (growing daily) of photos and videos.

The design was inspired by [Backblaze's Vault architecture](https://www.backblaze.com/blog/vault-cloud-storage-architecture/).
Development would have not been possible were it not for Backblaze's [open sourcing](https://github.com/Backblaze/JavaReedSolomon)
of their Reed-Solomon implementation and [@ronomon's subsequent port](https://github.com/ronomon/reed-solomon) to Node.js.

STRUBS is still heavily under development and this README will be updated as the product matures.