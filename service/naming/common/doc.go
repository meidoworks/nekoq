package common

/*

1. path format:
[a-zA-Z0-9_.]

2. full path format:
(path):(sub path):...:[(sub path)|(temp path)]

3. quorum:
    a. NamingTree
    b. SessionManager
    c. TempPath
    d. data/attr

4. across quorum:
    a. Observer mode
    b. client mode
    c. transfer mode

5. quorum auth:
    node identity/key, not ip
    communication under ip

6. find a NamingTree:
    -> root NamingTree
     -> its quorum or observer
      -> find PathTree node
       -> get sub NamingTree info
        -> its quorum or observer
         -> ...

7. node role:
    1. quorum
    2. observer: need to config
    3. transfer: automatically
    4. topology redundant transfer: HA, turn on/off

8. quorum cache
    1. mem
    2. quorum change notify to all role nodes
    3. cache quorum info when first connect to quorum

*/
