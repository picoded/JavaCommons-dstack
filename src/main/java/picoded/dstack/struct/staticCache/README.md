# staticCache

Static based varient using cache2k.

This helps facilitate local application caching. Where the actual value map is stored perisistently in shared static namespace.
As such great care needs to be taken when loading this in shared JVM envrionment, to prevent namespace collisions.