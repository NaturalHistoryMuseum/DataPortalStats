# stats
Generate quarterly stats


Stats.db
========

The ckanpackager script stores all requests in an SQLite DB stats.db.

Inside docker, this is available at /var/lib/ckanpackager/stats.db.

Outside of docker, you will need to locate the location of the mounted volume.  

To do so, find the container ID of the docker nginx_deploy instance

docker ps

And then lookup the volume:

docker inspect [container id]

Look at the Volumes section fo the JSON - it's currently mounted at /var/lib/docker/vfs/dir/7d8538fc1a2c2d1947573d2b177728941d7573bf1645f10f356f16d695e71f54 on the host system.


