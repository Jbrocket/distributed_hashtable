# Distributed Hashtable
## Info 
This is a repository for my distributed hashtable which is able to run on n servers up to 5, 
but you can add more if you clone more server directories, and the hashtable makes up to k copies
up to n servers.

## Usage
To activate different servers, just call it as such:

```
python3 server.py <NAME-OF-SERVER>
```
### IMPORTANT
The server makes use of a catalogue server that keeps track of names, you can edit the code directly
so that you can either use your own name-server, or just keep track of your own HOST/PORT.

To start testing/using the servers, we invoke the cluster client using testbasics/py as such:
```
python3 <TestBasics.py|TestPerf.py> <NAME-OF-SERVER> <NUM-SERVERS> <K-COPIES>
```
TestBasics finds the servers with names <NAME-OF-SERVER-[0-NUM_SERVERS]> and uses the client
cluster so the cluster can determine which server to store keys/lookup/delete keys from.

## Features
The clusterclient takes advantage of multiple servers to add replication and persistence
for every key. In addition to this, since there are multiple servers that store the same key,
the cluster also makes use of random access to increase the total throughput of the system.
If, for whatever reason, the client can't access the server, it'll retry on writes, but on lookups
it chooses a new server to read from. Each server maintains its own transaction file and checkpoint
to maintain persistence of data.
