# P2P-network
Peer-to-Peer Network using Distributed Hash Table


The DHT server can be run by running:-
```
python test.py tmux
```

tmux is the preferred way.
Incase, tmux is not available or it is required to run the code in xterm, run:-
```
python test.py xterm
```
The format to add new peers is:-
```
xterm -hold -title "Peer 2" -e "python main.py init 2 4 5 30"&
```
or
```
tmux split-window -v "python main.py init 14 19 2 30"
tmux select-layout tiled
tmux attach-session -t ass
```

tmux session can be detached using key combination ctrl+b and then d. Then session can be killed after detaching using:-
```
tmux kill-session -t ass
```

Further operations can be given directly in the peer terminal.
