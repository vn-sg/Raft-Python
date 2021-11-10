#!/bin/python3

# timeout       float 0.15 - 0.30
# term          int
# state         int
# leader        int
# log           list
# commitIndex   int
import sys
import time
import random
import threading


class message:
    AE = "AppendEntries"
    AER = "AppendEntriesResponse"
    RV = "RequestVotes"
    RVR = "RequestVotesResponse"

class state:
    L = "LEADER"
    F = "FOLLOWER"
    C = "CANDIDATE"

class node:

    def __init__(self, nodeID, nodeCount):
        self.vote_for = -1
        self.nodeCount = int(nodeCount)
        self.nodeID = int(nodeID)
        self.majority = int(int(nodeCount)/2) + 1
        self.timeout = timeout_gen()
        self.term = 1
        self.state = state.F
        self.timer = None
        self.heartbeatSender = None
        self.log = []
        self.votedBy = []
        self.leader = -1
    
    def start(self):
        state_tracker = threading.Thread(target=self.print_state, args=())
        state_tracker.start()
        self.handle_msg()
        
    def reset_timeout(self):
        if self.timer:
            self.timer.cancel()
        self.timeout = timeout_gen()
        self.timer = threading.Timer(self.timeout, self.timeout_action,)
        self.timer.start()

    def AE_handler(self, senderID=-1, term=-1, index=-1):
        # print(f"@RECEIVE sender:{senderID}\tterm:{term}\tindex:{index}")
        # Receiving AppendEntries from higher-term Node
        if term > self.term:
            # set the term to be the same 
            # consider that node leader
            self.reset_timeout()
            self.term = term
            self.vote_for = -1
            self.leader = senderID
            self.state = state.F
            
        elif term == self.term:
            if self.state == state.F:
                self.reset_timeout()
                self.leader = senderID
                # self.vote_for = senderID
                # Reply True with current term
                print(f"SEND {senderID} {message.AER} {self.term} true")
            else:
                # Reply True with current term
                self.reset_timeout()
                self.leader = senderID
                self.state = state.F
                
                # self.vote_for = senderID
                print(f"SEND {senderID} {message.AER} {self.term} true")
        else:
            # Reply False and notify our current term
            print(f"SEND {senderID} {message.AER} {self.term} false")

    def AER_handler(self, senderID=-1, term=-1, is_accepted="false"):
        if term > self.term:
            # self.reset_timeout()
            self.term = term
            self.vote_for = -1
            self.leader = -1
            self.state = state.F
        # elif term == self.term:
        #     if self.state == state.L:
        #         pass
        #     if self.state == state    
    
    def RV_handler(self, senderID=-1, term=-1):
        if term > self.term:
            self.reset_timeout()
            self.term = term
            self.vote_for = senderID
            self.leader = -1
            self.state = state.F
            print(f"SEND {senderID} {message.RVR} {term} true")
        elif term == self.term:
            if self.state == state.L:
                print(f"SEND {senderID} {message.RVR} {self.term} false")
            else:
                if self.vote_for == -1:
                    self.vote_for = senderID
                    self.leader = -1
                    self.state = state.F
                    print(f"SEND {senderID} {message.RVR} {term} true")
                else:
                    print(f"SEND {senderID} {message.RVR} {self.term} false")
        else:
            print(f"SEND {senderID} {message.RVR} {self.term} false")

    def RVR_handler(self, senderID=-1, term=-1, is_accepted="false"):
        if term > self.term:
            # self.reset_timeout()
            self.term = term
            self.vote_for = -1
            self.leader = -1
            self.state = state.F
        elif term == self.term:
            if self.state == state.C and is_accepted == "true":
                self.votedBy.append(senderID)
                if len(self.votedBy) >= self.majority:
                    self.timer.cancel()
                    self.timer = None
                    self.leader = self.nodeID
                    self.state = state.L
                    
                    for id in range(self.nodeCount):
                        if id != self.nodeID:
                            print(f"SEND {id} {message.AE} {self.term} 0")
                    self.heartbeatSender = threading.Timer(0.08, self.heartbeat,)
                    self.heartbeatSender.start()
                    self.timeout = -1

    def heartbeat(self):
        if self.state != state.L:
            self.heartbeatSender = None
            return
        for id in range(self.nodeCount):
            if id != self.nodeID:
                print(f"SEND {id} {message.AE} {self.term} 0")
        self.heartbeatSender = threading.Timer(0.08, self.heartbeat,)
        self.heartbeatSender.start()
        return

    def handle_msg(self):
        self.timer = threading.Timer(self.timeout, self.timeout_action,)
        self.timer.start()
        while True:
            # self.timer.start()
            msg = sys.stdin.readline().split()
            if len(msg) > 2:
                if msg[2] == message.AE:
                    self.AE_handler(senderID=int(msg[1]), term=int(msg[3]), index=int(msg[4]))
                elif msg[2] == message.AER:
                    self.AER_handler(senderID=int(msg[1]), term=int(msg[3]), is_accepted=msg[4])
                elif msg[2] == message.RV:
                    self.RV_handler(senderID=int(msg[1]), term=int(msg[3]))
                elif msg[2] == message.RVR:
                    self.RVR_handler(senderID=int(msg[1]), term=int(msg[3]), is_accepted=msg[4])
                else:
                    print("WRONG INPUT!!")

            # self.timer.cancel()
            # self.timer = threading.Timer(self.timeout, self.timeout_action,)
            # self.timer.start()
            
            # print(f"Term: {self.term}\nSTATE: {self.state}\nTIMEOUT: {self.timeout}")
    
    def timeout_action(self):
        # print(time.time())
        self.term += 1 
        self.leader = -1
        self.state = state.C
        for i in range(self.nodeCount):
            if i != self.nodeID:
                print(f"SEND {i} {message.RV} {self.term}")
        
        self.votedBy= [self.nodeID]
        self.vote_for = self.nodeID
        new_timeout = timeout_gen()
        self.timeout = new_timeout
        self.timer = threading.Timer(new_timeout, self.timeout_action,)
        self.timer.start()
        return

    def print_state(self):
        cur_state = self.state
        while True:
            if cur_state != self.state:
                cur_state = self.state
                print(f"STATE state=\"{self.state}\"")
                print(f"STATE term={self.term}")
                if self.leader != -1:
                    print(f"STATE leader={self.leader}")


def timeout_gen():
    return random.uniform(0.5, 0.8)

if __name__ == "__main__":
    _, nodeID, nodeCount = sys.argv
    n = node(nodeID, nodeCount)
    n.start()
