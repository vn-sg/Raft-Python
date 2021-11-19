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
import json

HEARTBEAT_INTERVAL = 0.8

def timeout_gen():
    return random.uniform(1.5, 1.8)

class message:
    AE = "AppendEntries"
    AER = "AppendEntriesResponse"
    RV = "RequestVotes"
    RVR = "RequestVotesResponse"

class state:
    L = "LEADER"
    F = "FOLLOWER"
    C = "CANDIDATE"

class dictionary_template:
    def __init__(self, term=0, response="-1", prev_index=0, prev_term=0, commit_index=0, log=[], lastLogIndex=0, lastLogTerm=0, matchIndex=-1):
        self.term = term
        self.response = response
        self.prev_index = prev_index
        self.prev_term = prev_term
        self.commit_index = commit_index
        self.log = log
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm
        self.matchIndex = matchIndex

    def get_dict(self):
        new_dict = {}
        new_dict["term"] = self.term
        new_dict["response"] = self.response
        new_dict["prev_index"] = self.prev_index
        new_dict["prev_term"] = self.prev_term
        new_dict["commit_index"] = self.commit_index
        new_dict["log"] = self.log
        new_dict["lastLogIndex"] = self.lastLogIndex
        new_dict["lastLogTerm"] = self.lastLogTerm
        new_dict["matchIndex"] = self.matchIndex
        return new_dict


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
        self.heartbeatSender = [None for _ in range(self.nodeCount)]
        self.log = []
        self.votedBy = []
        self.leader = -1

        self.commitIndex = -1
        # Index of the next log entry to send to that server
        # (initialized to leader last log index + 1)
        self.nextIndex = [0 for _ in range(self.nodeCount)]
        # Index of highest log entry known to be replicated on server
        # (initialized to 0, increases monotonically)
        self.matchIndex = [0 for _ in range(self.nodeCount)]
    
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

    def cancel_heartbeat(self):
         if self.state != state.L:
            for i in self.heartbeatSender:
                if i:
                    i.cancel()
            self.heartbeatSender = [None for _ in range(self.nodeCount)]
    
    def reset_index(self):
        self.nextIndex = [len(self.log) for _ in range(self.nodeCount)]
        self.matchIndex = [-1 for _ in range(self.nodeCount)]

    def AE_handler(self, senderID, msg_dict):
        # print(f"@RECEIVE sender:{senderID}\tterm:{term}\tindex:{index}")
        # Receiving AppendEntries from higher-term Node
        term = msg_dict["term"]
        if term > self.term:
            # set the term to be the same 
            # consider that node leader
            self.reset_timeout()
            self.term = term
            self.vote_for = -1
            self.leader = senderID
            self.state = state.F
            # self.cancel_heartbeat()
            prev_index = msg_dict["prev_index"]
            prev_term = msg_dict["prev_term"]
            commit_index = msg_dict["commit_index"]
            # log = 
            if prev_index == -1 or (len(self.log) > prev_index and self.log[prev_index][0] == prev_term):
                matchIndex = prev_index
                if(msg_dict["log"]):
                    if len(self.log) > prev_index + 1:
                        self.log = self.log[:prev_index+1]
                        self.log.append(msg_dict["log"])
                    else:
                        self.log.append(msg_dict["log"])
                    print(f"STATE log[{len(self.log)}]=[{self.log[-1][0]},\"{self.log[-1][1]}\"]")
                    matchIndex += 1
                if commit_index > self.commitIndex and msg_dict["log"]:
                    self.commitIndex = max(min(prev_index + 1, commit_index), self.commitIndex)
                    # if self.commitIndex != -1:
                    print(f"STATE commitIndex={self.commitIndex+1}")
                if not msg_dict["log"]:
                    self.commitIndex = max(min(prev_index, commit_index), self.commitIndex)
                    # if self.commitIndex != -1:
                    print(f"STATE commitIndex={self.commitIndex+1}")
                msg = dictionary_template(term=self.term, response="true", matchIndex=matchIndex).get_dict()
                # time.sleep(1)
                print(f"SEND {senderID} {message.AER} {json.dumps(msg)}")
            else:
                msg = dictionary_template(term=self.term, response="false").get_dict()
                print(f"SEND {senderID} {message.AER} {json.dumps(msg)}")
            
        elif term == self.term:
            # if self.state == state.F and self.leader == -1:
            if self.leader == -1 or self.leader == senderID:
                self.reset_timeout()
                self.leader = senderID
                self.state = state.F
                # self.vote_for = senderID
                # Reply True with current term
                prev_index = msg_dict["prev_index"]
                prev_term = msg_dict["prev_term"]
                commit_index = msg_dict["commit_index"]
                # log = 
                if prev_index == -1 or (len(self.log) > prev_index and self.log[prev_index][0] == prev_term):
                    matchIndex = prev_index
                    if(msg_dict["log"]):
                        if len(self.log) > prev_index + 1:
                            if self.log[prev_index+1][0] != msg_dict["log"][0]:
                                self.log = self.log[:prev_index+1]
                                self.log.append(msg_dict["log"])
                        else:
                            self.log.append(msg_dict["log"])
                        print(f"STATE log[{len(self.log)}]=[{self.log[-1][0]},\"{self.log[-1][1]}\"]")
                        matchIndex += 1 
                    if commit_index > self.commitIndex and msg_dict["log"]:
                        self.commitIndex = max(min(prev_index + 1, commit_index), self.commitIndex)
                        # if self.commitIndex != -1:
                        print(f"STATE commitIndex={self.commitIndex+1}")
                    if not msg_dict["log"]:
                        self.commitIndex = max(min(prev_index, commit_index), self.commitIndex)
                        # if self.commitIndex != -1:
                        print(f"STATE commitIndex={self.commitIndex+1}")
                    
                    msg = dictionary_template(term=self.term, response="true", matchIndex=matchIndex).get_dict()
                    print(f"SEND {senderID} {message.AER} {json.dumps(msg)}")
                else:
                    msg = dictionary_template(term=self.term, response="false").get_dict()
                    print(f"SEND {senderID} {message.AER} {json.dumps(msg)}")

                
                # print(f"SEND {senderID} {message.AER} {self.term} true")
            # elif self.leader == senderID:
            #     # Reply True with current term
            #     self.reset_timeout()
            #     # l = threading.Lock()
            #     # l.acquire()
            #     self.leader = senderID
            #     self.state = state.F
            #     # l.release()
            #     # self.cancel_heartbeat()
                
            #     # self.vote_for = senderID
            #     msg = dictionary_template(term=self.term, response="true").get_dict()
            #     print(f"SEND {senderID} {message.AER} {json.dumps(msg)}")
            #     # print(f"SEND {senderID} {message.AER} {self.term} true")
            else:
                msg = dictionary_template(term=self.term, response="false").get_dict()
                print(f"SEND {senderID} {message.AER} {json.dumps(msg)}")
                return

        else:
            # Reply False and notify our current term
            msg = dictionary_template(term=self.term, response="false").get_dict()
            print(f"SEND {senderID} {message.AER} {json.dumps(msg)}")
            # print(f"SEND {senderID} {message.AER} {self.term} false")

    def AER_handler(self, senderID, msg_dict):
        term = msg_dict["term"]
        response = msg_dict["response"]
        matchIndex = msg_dict["matchIndex"]
        if term > self.term:
            # self.reset_timeout()
            self.term = term
            self.vote_for = -1
            self.leader = -1
            self.state = state.F
        elif term == self.term:
            if self.state == state.L:
                if response == "false":
                    self.nextIndex[senderID] -= 1
                    self.LOG_processor(senderID)
                else:
                    # if self.nextIndex[senderID] < len(self.log):
                        # self.nextIndex[senderID] += 1
                    self.nextIndex[senderID] = matchIndex + 1
                    self.matchIndex[senderID] = matchIndex
                    # self.matchIndex[senderID] = self.nextIndex[senderID] - 1
                    if self.matchIndex[senderID] != len(self.log) - 1:
                        self.LOG_processor(senderID)
                    
                    
                    sort_list = [self.matchIndex[i] for i in range(self.nodeCount) if i != self.nodeID] + [len(self.log)-1]
                    sort_list.sort(reverse=True)
                    new_commitIndex = sort_list[self.majority-1]
                    # sys.stderr.write(f"sort_list is {sort_list}, and new_commitIndex is {new_commitIndex} with index {self.majority-1}")
                    if self.log and self.commitIndex < new_commitIndex and self.log[new_commitIndex][0] == self.term:
                        self.commitIndex = new_commitIndex
                        print(f"STATE commitIndex={self.commitIndex+1}")
    
    def RV_handler(self, senderID, msg_dict):
        term = msg_dict["term"]
        lastLogIndex = msg_dict["lastLogIndex"]
        lastLogTerm = msg_dict["lastLogTerm"]
        if term > self.term:
            self.reset_timeout()
            self.term = term
            self.state = state.F
            self.leader = -1
            # self.cancel_heartbeat()
            if not self.log:
                self.vote_for = senderID
                msg = dictionary_template(term=self.term, response="true").get_dict()
                print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
                return
            if lastLogTerm < self.log[-1][0] or (lastLogTerm == self.log[-1][0] and lastLogIndex < len(self.log)-1):
                msg = dictionary_template(term=self.term, response="false").get_dict()
                print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
            else:
                self.vote_for = senderID
                msg = dictionary_template(term=self.term, response="true").get_dict()
                print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
            # print(f"SEND {senderID} {message.RVR} {term} true")
        elif term == self.term:
            if self.state == state.L:
                msg = dictionary_template(term=self.term, response="false").get_dict()
                print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
                # print(f"SEND {senderID} {message.RVR} {self.term} false")
            else:
                if self.vote_for == -1 and self.leader == -1:
                    if not self.log:
                        self.vote_for = senderID
                        msg = dictionary_template(term=self.term, response="true").get_dict()
                        print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
                        return
                    if lastLogTerm < self.log[-1][0] or (lastLogTerm == self.log[-1][0] and lastLogIndex < len(self.log)-1):
                        msg = dictionary_template(term=self.term, response="false").get_dict()
                        print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
                        return
                    
                    self.vote_for = senderID
                    # l = threading.Lock()
                    # l.acquire()
                    # self.leader = -1
                    self.state = state.F
                    # l.release()
                    # self.cancel_heartbeat()
                    msg = dictionary_template(term=self.term, response="true").get_dict()
                    print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
                        # else:
                        #     msg = dictionary_template(term=self.term, response="false").get_dict()
                        #     print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
                    # print(f"SEND {senderID} {message.RVR} {term} true")
                else:
                    msg = dictionary_template(term=self.term, response="false").get_dict()
                    print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
                    # print(f"SEND {senderID} {message.RVR} {self.term} false")
        else:
            msg = dictionary_template(term=self.term, response="false").get_dict()
            print(f"SEND {senderID} {message.RVR} {json.dumps(msg)}")
            # print(f"SEND {senderID} {message.RVR} {self.term} false")

    def RVR_handler(self, senderID=-1, term=-1, is_accepted="false"):
        if term > self.term:
            # self.reset_timeout()
            self.term = term
            self.vote_for = -1
            self.leader = -1
            self.state = state.F
            # self.cancel_heartbeat()
        ### I'm elected as the leader
        elif term == self.term:
            if self.state == state.C and is_accepted == "true":
                self.votedBy.append(senderID)
                if len(self.votedBy) >= self.majority:
                    self.timer.cancel()
                    self.timer = None
                    self.leader = self.nodeID
                    self.state = state.L
                    self.reset_index()
                    for id in range(self.nodeCount):
                        if id != self.nodeID:
                            self.LOG_processor(id)
                            # print(f"SEND {id} {message.AE} {self.term} 0")
                            self.heartbeatSender[id] = threading.Timer(HEARTBEAT_INTERVAL, self.heartbeat, args=(id,))
                            self.heartbeatSender[id].start()
                    self.timeout = -1

    def LOG_processor(self, ID):
        # self.nextIndex[ID] -= 1
        # try:
        #     self.heartbeatSender[ID].cancel()
        #     self.heartbeatSender[ID] = threading.Timer(HEARTBEAT_INTERVAL, self.heartbeat, args=(ID,))
        #     self.heartbeatSender[ID].start()
        # except:
        #     pass
        if len(self.log) == 0:
            msg = dictionary_template(term=self.term, prev_index=-1, prev_term=-1, log=None, commit_index=-1).get_dict()
        else:
            # sys.stderr.write(f"self.nextIndex[ID] is {self.nextIndex[ID]}, and length of self.log is {len(self.log)}")
            prevIndex = self.nextIndex[ID]-1
            log_entry = self.log[prevIndex]
            
            if self.nextIndex[ID] != (self.matchIndex[ID] + 1) or self.matchIndex[ID] == len(self.log)-1:
                msg = dictionary_template(term=self.term, prev_index=prevIndex, prev_term=log_entry[0], log=None, commit_index=self.commitIndex).get_dict()
            else:
                msg = dictionary_template(term=self.term, prev_index=prevIndex, prev_term=log_entry[0], log=self.log[self.nextIndex[ID]], commit_index=self.commitIndex).get_dict() 
        print(f"SEND {ID} {message.AE} {json.dumps(msg)}")

        
        

    def LOG_handler(self, msg):
        self.log.append([self.term, msg.split()[0]])
        print(f"STATE log[{len(self.log)}]=[{self.log[-1][0]},\"{self.log[-1][1]}\"]")

    def heartbeat(self, followerID):
        # sys.stderr.write(f"%%node: {self.nodeID}\t state:{self.state}%%\t BEAT FOR: {followerID}\n")
        
        if self.state != state.L:
            # sys.stderr.write(f"HEARTBEAT FOR follower:{followerID} has been canceled on node: {self.nodeID}!")
            self.heartbeatSender[followerID] = None
            return
        # for id in range(self.nodeCount):
        #     if id != self.nodeID:
        # msg = dictionary_template(term=self.term, prev_index=0).get_dict()
        # print(f"SEND {followerID} {message.AE} {json.dumps(msg)}")
        self.LOG_processor(followerID)
        # print(f"SEND {followerID} {message.AE} {self.term} 0")
        self.heartbeatSender[followerID] = threading.Timer(HEARTBEAT_INTERVAL, self.heartbeat, args=(followerID,))
        self.heartbeatSender[followerID].start()
        return

    def handle_msg(self):
        self.timer = threading.Timer(self.timeout, self.timeout_action,)
        self.timer.start()
        while True:
            # self.timer.start()
            msg = sys.stdin.readline().split(" ", 3)
            # for i in range(len(msg)):
            #     sys.stderr.write(f"[{i}]:\t" + msg[i])    
            # sys.stderr.write(" ".join(msg))
            if msg:
                if msg[0] == "LOG":
                    # sys.stderr.write(f"RECEIVED LOG: {msg}")
                    if self.state != state.L:
                        continue
                    else:
                        # self.LOG_handler(json.loads(msg[1])[0])
                        self.LOG_handler(msg[1])
                    continue

                try:
                    msg_dict = json.loads(msg[-1])
                except:
                    continue
                if len(msg) > 2:
                    if msg[2] == message.AE:
                        self.AE_handler(int(msg[1]), msg_dict)
                        # self.AE_handler(senderID=int(msg[1]), term=int(msg[3]), index=int(msg[4]) if msg[4].isdigit() else int(''.join(e for e in msg[4] if e.isdigit())))
                    elif msg[2] == message.AER:
                        self.AER_handler(int(msg[1]), msg_dict)
                        # self.AER_handler(senderID=int(msg[1]), term=int(msg_dict["term"]), is_accepted=msg_dict["response"])
                        # self.AER_handler(senderID=int(msg[1]), term=int(msg[3]), is_accepted=msg[4])
                    elif msg[2] == message.RV:
                        self.RV_handler(int(msg[1]), msg_dict)
                        # self.RV_handler(senderID=int(msg[1]), term=int(msg_dict["term"]))
                        # self.RV_handler(senderID=int(msg[1]), term=int(msg[3]) if msg[3].isdigit() else int(''.join(e for e in msg[3] if e.isdigit())))
                    elif msg[2] == message.RVR:
                        self.RVR_handler(senderID=int(msg[1]), term=int(msg_dict["term"]), is_accepted=msg_dict["response"])
                        # self.RVR_handler(senderID=int(msg[1]), term=int(msg[3]), is_accepted=msg[4] )
                
                else:
                    sys.stderr.write("WRONG INPUT!!")                                             
            else:
                sys.stderr.write("EMPTY MESSAGE!!")

            # self.timer.cancel()
            # self.timer = threading.Timer(self.timeout, self.timeout_action,)
            # self.timer.start()
            
            # print(f"Term: {self.term}\nSTATE: {self.state}\nTIMEOUT: {self.timeout}")
    
    def timeout_action(self):
        # print(time.time())
        self.term += 1 
        self.leader = -1
        self.state = state.C
        # self.cancel_heartbeat()
        for i in range(self.nodeCount):
            if i != self.nodeID:
                msg = dictionary_template(term=self.term, lastLogIndex=len(self.log)-1, lastLogTerm=self.log[-1][0] if self.log else -1).get_dict()
                print(f"SEND {i} {message.RV} {json.dumps(msg)}")
                # print(f"SEND {i} {message.RV} {self.term}")
        
        self.votedBy= [self.nodeID]
        self.vote_for = self.nodeID
        new_timeout = timeout_gen()
        self.timeout = new_timeout
        self.timer = threading.Timer(new_timeout, self.timeout_action,)
        self.timer.start()
        return

    def print_state(self):
        cur_state = self.state
        cur_leader = self.leader

        while True:
            
            if cur_state != self.state or cur_leader != self.leader:
                cur_leader = self.leader
                cur_state = self.state
                print(f"STATE term=\"{self.term}\"")
                if self.leader != -1:
                    print(f"STATE leader={self.leader}")
                # else:
                #     print(f"STATE leader=null")
                print(f"STATE state=\"{self.state}\"")
            # if len(log) != len(self.log) or log[-1][0] != self.log[-1][0]:
            #     log = self.log
            #     print(f"STATE log=\"{self.log[-1]}\"")
                


            # if commitIndex != self.commitIndex:
            #     commitIndex = self.commitIndex

                

if __name__ == "__main__":
    _, nodeID, nodeCount = sys.argv
    n = node(nodeID, nodeCount)
    n.start()


# from __future__ import print_function

# import sys

# def eprint(*args, **kwargs):
#     print(*args, file=sys.stderr, **kwargs)

# eprint("fail")

# import sys
# sys.stderr.write()