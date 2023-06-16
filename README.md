# The LotMan Library

## Introduction

The LotMan library is intended to serve as a type of accountant for data use/storage in high-througput systems where there is desire to be using as much storage as possible, but in a fair manner, such as in the case of caches within the Open Science Data Federation ([OSDF](https://osdf.osg-htc.org/)). Its core novelty is the creation of, and ability to reason over, the “lot” object, described in more detail below.

The primary goal of the library is to answer questions about the way storage is being used, by whom, and for how long in systems where limited storage resources would otherwise require convoluted management of user quotas. It should be noted that LotMan itself is not responsible for the creation or deletion of any data other than the data associated with establishing the lots themselves. Instead, it provides information to external applications who can then implement data deletion and retention policies based on LotMan's advice.


## Lot Attributes

Lot objects are comprised of several components:
- **Name:** Lots are identified by their names. Every lot must be given a unique name. The lot with name "default" is considered special, and must be created before any other lots.
- **Owner:** Every lot has an associated data owner. The key distinction to make here is that while the data owner owns the data associated with a lot, they do not necessarily own the lot itself\. Rather, ownership of a lot may be likened to growing vegetables in a rented garden -- you own what you grow, but not the dirt you grow it in.
- **Parents:** Every lot must have at least one parent. Parent/child relationships are used for calculating lot usage statistics and identifying lots that may be in violation of their management policy attributes. When a lot is assigned children, it is signifying that the parent not only owns the data associated with those lots, but that it also owns those lots themselves, which gives the owner of the parent lot the ability to modify parameters of the children lots. In the case that a lot is a self parent, the owner of the lot is also able to modify attributes for the lot itself. When querying LotMan for usage statistics, the usage of a lot's children *may* be counted toward the quotas of the parent; queries can be made to return information only about lots themselves, or about lots and their children. Lots with only themselves as parents are *root* lots.
- **Paths:** The list of paths/objects tied to the lot whose statistics should be tracked. One design consideration of LotMan is that these paths need not be rigidly tied to traditional filesystems. While the term *paths* is natural in the context of filesystems, LotMan can also use the URI of any object as a path. When a path is associated with a lot, it can be done so either recursively or non-recursively. If recursive is set to true, it indicates that any sub directories should also be attributed to a lot. For example, if path `/foo` is explicitly tied to a lot with recursive set to true, then `/foo/bar` is as well such that `/foo/bar` cannot be tied to another lot. Conversely, if `/foo` is tied to a lot with recursive set to false, then `/foo/bar` may be tied to another lot. When querying LotMan for information about a path that is not explicitly tied to a lot, LotMan will treat that path as belonging to the default lot. 
- **Management Policy Attributes (MPAs):** These are the attributes that can be used to make decisions about a lot and its associated data. They are:
    - *Creation Time* -- The unix epoch timestamp in milliseconds at which a lot becomes valid. 
    - *Expiration Time* -- The unix epoch timestamp in milliseconds at which a lot expires. Expired lots and their associated data should be considered transient. That is, the owner of the system's storage resources *may* choose to allow a lot to continue using resources if resources are abundant, but the lot's owner should have no expectations.
    - *Deletion Time* -- The unix epoch timestamp in milliseconds at which a lot and its associated data should be deleted. 
    - *Dedicated GB* -- The amount of storage made available to the lot owner. Owners who stay within this limit should be guaranteed this amount of storage while the lot is still viable.
    - *Opportunistic GB* -- Once a lot has used its entire allotment of dedicated storage, data is counted toward its opportunistic storage. Similar to expired lots, a system *may* make opportunistic storage available to the lot when resources are abundant. However, because LotMan intentionally does not track which paths associated with a lot are tied to different types of storage, when a system must make space, it must make a decision about which files from the lot are to be deleted. For this reason, exceeding dedicated storage limits should be treated as making any portion of the lot's associated data transient.
    - *Max Objects* -- The maximum number of objects a lot can store. 
- **Usage Statistics:** Several usage statistics can be tracked for each lot. They are:
    - *Self GB* -- The number of GB a lot is currently using, not including those of its children.
    - *Children GB* -- The cumulative number of GB being used by all of a lot's children, not including itself in cases where a lot is a self parent.
    - *Self Objects* -- The number of objects a lot currently possesses, not including the objects possessed by its children.
    - *Children Objects* -- The cumulative number of objects possessed by all of the lots children, not including itself in cases where a lot is a self parent.
    - *Self GB Being Written* -- The number of GB associated with a lot being written to disk, not including its children.
    - *Children GB Being Written* -- The number of GB associated with a lot's children being written to disk, not including itself in cases where a lot is a self parent.
    - *Self Objects Being Written* -- The number of objects associated with a lot being written to disk, not including those of its children.
    - *Children Objects Being Written* -- The number of objects associated with a lot's children being written to disk, not including itself in cases where a lot is a self parent.

## Example Usage Scenario

One scenario in which LotMan's features becomes particularly relevant is in the case of data caches, where the desire is to be using as much system storage as possible (which is arguably the cache's job). In this case, the cache may be configured to start clearing files after storage use reaches a certain threshold, perhaps until storage use dips below a separate threshold -- a high watermark and low watermark scheme. If the cache is configured to use LotMan, then when it comes time to delete files, it can implement a priority-based deletion loop. For example, it may first ask LotMan for all the paths associated with lots past their deletion point, choosing to delete those files first. Until the low watermark has been reached, it may then ask for paths associated with lots past their expiration time, past their opportunistic storage, past their max number of objects, and past their dedicated storage. For each query, LotMan is capable of returning all of the paths associated with any lot that meets the supplied criteria, including whether to count children statistics toward the lot's quoats.

## Building

To build the LotMan library, the following dependencies are needed:
- The `[nlohmann/json](https://github.com/nlohmann/json)` header-only library, which LotMan uses for working with JSON
- The `[pboettch/json-schema-validator](https://github.com/pboettch/json-schema-validator)` header-only library, which LotMan uses for validating JSON schemas
- `sqlite3`

Once the repo is cloned, it can be built by following:
```
mkdir build
cd build
cmake ..
make

# only for installing on the system
# make install
```

