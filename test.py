import os
import sys
import time


def test_xterm():
    # Initial Peers
    os.system('xterm -hold -title "Peer 2" -e "python main.py init 2 4 5 30"&')
    os.system('xterm -hold -title "Peer 4" -e "python main.py init 4 5 8 30"&')
    os.system('xterm -hold -title "Peer 5" -e "python main.py init 5 8 9 30"&')
    os.system('xterm -hold -title "Peer 8" -e "python main.py init 8 9 14 30"&')
    os.system('xterm -hold -title "Peer 9" -e "python main.py init 9 14 19 30"&')
    os.system('xterm -hold -title "Peer 14" -e "python main.py init 14 19 2 30"&')
    os.system('xterm -hold -title "Peer 19" -e "python main.py init 19 2 4 30"&')

    # Add new peer
    time.sleep(5)
    os.system('xterm -hold -title "Peer 15" -e "python main.py join 15 4 30"&')
    return

def test_tmux():
    # Initial peers
    os.system('tmux new -d -s ass "python main.py init 2 4 5 30"')
    os.system('tmux split-window -v "python main.py init 4 5 8 30"')
    os.system('tmux split-window -h "python main.py init 5 8 9 30"')
    os.system('tmux split-window -v "python main.py init 8 9 14 30"')
    os.system('tmux split-window -h "python main.py init 9 14 19 30"')
    os.system('tmux split-window -v "python main.py init 14 19 2 30"')
    os.system('tmux split-window -h "python main.py init 19 2 4 30"')

    # Add new peer
    time.sleep(2)
    os.system('tmux split-window -h "python main.py join 15 4 30"')

    os.system("tmux set-option -w -t :ass remain-on-exit on")
    os.system('tmux select-layout tiled')
    os.system('tmux attach-session -t ass')
    return


if __name__ == "__main__":
    if sys.argv[1] == "tmux":
        test_tmux()
    elif sys.argv[1] == "xterm":
        test_xterm()
    else:
        print("Enter valid input.")
