# coding=utf-8
# ------------------------------------------------------------------------------------------------------
# TDA596 - Lab 1
# server/server.py
# Input: Node_ID total_number_of_ID
# Student: John Doe
# ------------------------------------------------------------------------------------------------------
import traceback
import sys
import time
import json
import argparse
from threading import Thread, Timer
from random import randint
from bottle import Bottle, run, request, template
import requests

# ------------------------------------------------------------------------------------------------------
try:
    app = Bottle()

    #board stores all message on the system
    board = {0 : "Welcome to Distributed Systems Course"}

    # Indicates whether this node has received a take-over message
    # during an election.
    taken_over = False
    in_election = False
    # We need to define these timers at the start, since we might
    # want to cancel them before they've been properly defined.
    take_over_timer = Timer(1, lambda: None)

    # ------------------------------------------------------------------------------------------------------
    # BOARD FUNCTIONS
    # You will probably need to modify them
    # ------------------------------------------------------------------------------------------------------

    #This functions will add an new element
    def add_new_element_to_store(entry_sequence, element, is_propagated_call=False):
        global board
        success = False
        try:
            if entry_sequence not in board:
                board[entry_sequence] = element
                success = True
        except Exception as e:
            print e
        return success

    def modify_element_in_store(entry_sequence, modified_element, is_propagated_call = False):
        global board
        success = False
        try:
            if entry_sequence in board:
                board[entry_sequence] = modified_element
                success = True
        except Exception as e:
            print e
        return success

    def delete_element_from_store(entry_sequence, is_propagated_call = False):
        global board
        success = False
        try:
            if entry_sequence in board:
                del board[entry_sequence]
                success = True
        except Exception as e:
            print e
        return success

    def set_leader(ip):
        """Set the leader and end the election for this node."""
        global leader, taken_over, in_election
        print "LEADER IS SET"
        print "Leader before: {}".format(leader)
        leader = ip
        print "Leader after: {}".format(leader)
        taken_over = False
        in_election = False

    # ------------------------------------------------------------------------------------------------------
    # ROUTES
    # ------------------------------------------------------------------------------------------------------
    # a single example (index) for get, and one for post
    # ------------------------------------------------------------------------------------------------------
    #No need to modify this

    @app.route('/')
    def index():
        global board, node_id, timeout
        return template('server/index.tpl',
                        board_title='Vessel {}'.format(node_id),
                        board_dict=sorted({"0": board,}.iteritems()),
                        members_name_string='Thomas, Julio, Mihkel',
                        leader=leader)

    @app.get('/board')
    def get_board():
        global board, node_id
        print board
        return template('server/boardcontents_template.tpl',
                        board_title='Vessel {}'.format(node_id),
                        board_dict=sorted(board.iteritems()),
                        leader=leader)

    #------------------------------------------------------------------------------------------------------

    # You NEED to change the follow functions
    @app.post('/board')
    def client_add_received():
        '''Adds a new element to the board
        Called directly when a user is doing a POST request on /board'''
        global board, node_id, leader
        try:
            new_entry = request.forms.get('entry')

            # Forward the ADD action to the leader, if we are not the leader.
            if int(leader[-1]) != node_id:
                thread = Thread(target=contact_vessel,
                                args=(leader, "/board", {"entry": new_entry}, "POST"))
                thread.daemon = True
                thread.start()
                return True

            # Otherwise, we must be the leader, so we perform the usual actions.

            if len(board.keys()) == 0:
                element_id = 0
            else:
                element_id = list(board.keys())[-1] + 1 # you need to generate a entry number
            add_new_element_to_store(element_id, new_entry)

            # Propagate action to all other nodes example :
            thread = Thread(target=propagate_to_vessels,
                            args=('/propagate/ADD/' + str(element_id),
                                  {'entry': new_entry},
                                  'POST'))
            thread.daemon = True
            thread.start()
            return True
        except Exception as e:
            print (e)
        return False

    @app.post('/board/<element_id:int>')
    def client_action_received(element_id):
        global board, node_id, leader

        print "You receive an element"
        print "id is ", node_id
        # Get the entry from the HTTP body
        entry = request.forms.get('entry')

        delete_option = request.forms.get('delete')

        print "the delete option is ", delete_option

        # If we are not the leader, forward the action to the leader.
        if int(leader[-1]) != node_id:
            print("action to leader")
            thread = Thread(target=contact_vessel,
                            args=(leader,
                                  "/board/{}".format(element_id),
                                  {"entry": entry, "delete": delete_option},
                                  "POST"))
            thread.daemon = True
            thread.start()
            return True

        # Otherwise, if we are the leader, business as usual.

        # 0 = modify, 1 = delete
        if int(delete_option) == 0:
            modify_element_in_store(element_id, entry, False)
            action_str = 'MODIFY'
        elif int(delete_option) == 1:
            delete_element_from_store(element_id, False)
            action_str = 'DELETE'

        # Propagate to other nodes
        thread = Thread(target=propagate_to_vessels,
                        args=('/propagate/' + action_str + '/' + str(element_id),
                              {'entry': entry},
                              'POST'))
        thread.daemon = True
        thread.start()

    #With this function you handle requests from other nodes like add modify or delete
    @app.post('/propagate/<action>/<element_id:int>')
    def propagation_received(action, element_id):
        global taken_over, vessel_list, node_id, in_election

        # Get entry from http body
        entry = request.forms.get('entry')
        print "the action is", action

        # Handle requests
        if action == "ADD":
            add_new_element_to_store(element_id, entry, True)

        # Modify the board entry
        elif action == "MODIFY":
            modify_element_in_store(element_id, entry, True)

        # Delete the entry from the board
        elif action == "DELETE":
            delete_element_from_store(element_id, True)

        # Received from the new leader to announce the new leadership.
        elif action == "COORDINATOR":
            set_leader(vessel_list[str(element_id)])

        # Received from lower ID nodes during an election.
        elif action == "ELECTION":
            print "n: {}, sender: {}".format(entry, element_id)
            in_election = True
            # Send the take-over message back
            sender = element_id
            try:
                thread = Thread(target=contact_vessel,
                                args=(vessel_list[str(sender)],
                                      "/propagate/TAKEOVER/" + str(node_id),
                                      "POST"))
                thread.daemon = True
                thread.start()

            except Exception as e:
                pass

            if not taken_over:
                print("Continue bullying")
                bully()

        elif action == "TAKEOVER":
            # Received take-over response, we just wait for the new leader
            # to announce its election.
            print("Received takeover")
            taken_over = True


    # ------------------------------------------------------------------------------------------------------
    # DISTRIBUTED COMMUNICATIONS FUNCTIONS
    # ------------------------------------------------------------------------------------------------------
    def contact_vessel(vessel_ip, path, payload=None, req='POST'):
        """Try to contact another server (vessel) through a POST or GET, once.

        This function has built-in behaviour of starting an election if
        the leader failed to be contacted.
        """
        global in_election
        success = False
        try:
            print('CONTACTING PATH: http://{}{}'.format(vessel_ip, path))
            if 'POST' in req:
                res = requests.post('http://{}{}'.format(vessel_ip, path), data=payload)
            elif 'GET' in req:
                res = requests.get('http://{}{}'.format(vessel_ip, path))
            else:
                print 'Non implemented feature!'
            if res.status_code == 200:
                success = True
        except Exception as e:
            # We can not contact the current leader with a non-election request so we start new elections
            if vessel_ip == leader and 'ELECTION' not in path and not in_election:
                in_election = True
                bully()
            print e
        return success

    def propagate_to_vessels(path, payload = None, req = 'POST'):
        global vessel_list, node_id, leader

        for vessel_id, vessel_ip in vessel_list.items():
            if int(vessel_id) != node_id: # don't propagate to yourself
                success = contact_vessel(vessel_ip, path, payload, req)
                if not success:
                    print "Could not contact vessel {}\n".format(vessel_ip)

    def declare_itself_leader():
        # Set itself as the new leader, and propagate it to all other nodes
        global node_id, vessel_list
        propagate_to_vessels("/propagate/COORDINATOR/" + str(node_id))
        set_leader(vessel_list[str(node_id)])

    def bully():
        # Run the bully algorithm for one node
        global vessel_list, leader, node_id, taken_over, take_over_timer

        # Send the election message to all nodes with a higher ID.
        for n in sorted(vessel_list.keys())[node_id:]:
            print "Contact {} ({}) for election. PATH: {}".format(n, vessel_list[n], "/propagate/ELECTION/"+str(node_id))
            thread = Thread(target=contact_vessel,
                            args=(vessel_list[n],
                                  "/propagate/ELECTION/" + str(node_id),
                                  'POST'))
            thread.daemon = True
            thread.start()

        # After a timer we check if we are the new leader; if we are, we
        # announce that to all other nodes.
        def check_if_leader():
            global taken_over
            if taken_over:
                return 1
            elif in_election and not taken_over:
                declare_itself_leader()

        take_over_timer.cancel()
        take_over_timer = Timer(3, check_if_leader)
        take_over_timer.start()

    # ------------------------------------------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------------------------------------------
    def main():
        global vessel_list, node_id, app, leader

        port = 80
        parser = argparse.ArgumentParser(description='Your own implementation of the distributed blackboard')
        parser.add_argument('--id', nargs='?', dest='nid', default=1, type=int, help='This server ID')
        parser.add_argument('--vessels', nargs='?', dest='nbv', default=1, type=int, help='The total number of vessels present in the system')
        args = parser.parse_args()
        node_id = args.nid
        vessel_list = dict()
        # We need to write the other vessels IP, based on the knowledge of their number
        for i in range(1, args.nbv+1):
            vessel_list[str(i)] = '10.1.0.{}'.format(str(i))

        # Pick an arbitrary node to be the initial leader.
        leader = vessel_list.values()[0]

        try:
            run(app, host=vessel_list[str(node_id)], port=port)
        except Exception as e:
            print e
    # ------------------------------------------------------------------------------------------------------
    if __name__ == '__main__':
        main()


except Exception as e:
        traceback.print_exc()
        while True:
            time.sleep(60.)
