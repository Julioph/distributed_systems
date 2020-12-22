# coding=utf-8
# ------------------------------------------------------------------------------------------------------
# TDA596 - Lab 1
# server/server.py
# Input: Node_ID total_number_of_ID
# Student: Dream Team
# ------------------------------------------------------------------------------------------------------
import traceback
import sys
import time
import json
import argparse
from threading import Thread, Timer

from bottle import Bottle, run, request, template
import requests


class Action():
    def __init__(self, action_str, msg, msg_id, msg_counter = 0):
        # The action string is one of "ADD", "MODIFY", "DELETE".
        self.action_str = action_str
        # The message is:
        # - For "ADD": The message to be posted to the board.
        # - For "MODIFY": The new message (after modification).
        # - For "DELETE": None
        self.msg = msg
        # The message ID is an int.
        self.msg_id = msg_id
        self.msg_counter = msg_counter

    def __str__(self):
        return "The action string is: {}, MSG: {}, MSG_ID: {} and the counter is {}".format(self.action_str, self.msg, self.msg_id, self.msg_counter)

def update_counter(given_dict, key, value):
    success = False
    try:
        given_dict[key] = value
        success = True
    except Exception as e:
        print e
    return success

# ------------------------------------------------------------------------------------------------------
try:
    app = Bottle()

    # This board should be up to date with all other nodes. Every time
    # the node receives the propagation of some action, this global board
    # should be updated to reflect that.
    board = {0: "Welcome to Distributed Systems Course"}
    local_board = {}
    message_counter = {}

    # This is a queue of delete/modify actions posted at this node.
    # Whenever this node receives the mutex token, it will propagate
    # the action to all other nodes.
    action_queue = []

    # Queue for actions that do not have an applicable ID yet
    unmatched_queue = []


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

    def add_new_element_to_local_store(entry_sequence, element):
        global local_board
        success = False
        try:
            if entry_sequence not in local_board:
                local_board[entry_sequence] = element
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

    def modify_local_element_in_store(entry_sequence, modified_element):
        global local_board
        success = False
        try:
            if entry_sequence in local_board:
                local_board[entry_sequence] = modified_element
                success = True
        except Exception as e:
            print e
        return success

    def delete_element_from_store(entry_sequence, is_propagated_call = False):
        global board, message_counter
        success = False
        try:
            if entry_sequence in board:
                del board[entry_sequence]
                message_counter[entry_sequence] = -1
                success = True
        except Exception as e:
            print e
        return success

    def delete_element_from_local_store(entry_sequence, is_propagated_call = False):
        global local_board
        success = False
        try:
            if entry_sequence in local_board:
                del local_board[entry_sequence]
                success = True
        except Exception as e:
            print e
        return success

    # ------------------------------------------------------------------------------------------------------
    # ROUTES
    # ------------------------------------------------------------------------------------------------------
    # a single example (index) for get, and one for post
    # ------------------------------------------------------------------------------------------------------
    @app.route('/')
    def index():
        global board, node_id
        return template('server/index.tpl',
                        board_title='Vessel {}'.format(node_id),
                        board_dict=sorted(board.iteritems()),
                        members_name_string='Thomas, Julio, Mihkel',
                        local_dict=[])

    @app.get('/board')
    def get_board():
        global board, node_id
        print board
        return template('server/boardcontents_template.tpl',
                        board_title='Vessel {}'.format(node_id),
                        board_dict = sorted(board.iteritems()),
                        local_dict = sorted(local_board.iteritems()))

    #------------------------------------------------------------------------------------------------------

    # You NEED to change the follow functions
    @app.post('/board')
    def client_add_received():
        '''Adds a new element to the board
        Called directly when a user is doing a POST request on /board'''
        global board, node_id, my_token, local_board
        if my_token is not None:
            try:
                new_entry = request.forms.get('entry')

                element_id = my_token["last_msg_id"]
                my_token["last_msg_id"] += 1

                add_new_element_to_store(element_id, new_entry)

                # Propagate action to all other nodes example :
                thread = Thread(target=propagate_to_vessels,
                                args=('/propagate/ADD/' + str(element_id), {'entry': new_entry}, 'POST'))
                thread.daemon = True
                thread.start()
                return True
            except Exception as e:
                print (e, "here")
        else:
            new_entry = request.forms.get('entry')

            if len(local_board.keys()) == 0:
                element_id = 0
            else:
                element_id = list(local_board.keys())[-1] + 1

            add_new_element_to_local_store(element_id, new_entry)

        return False

    @app.post('/board/local/<element_id:int>/')
    def client_local_action_received(element_id):

        print "You receive a local element action"
        print "id is ", node_id
        # Get the entry from the HTTP body
        entry = request.forms.get('entry')

        delete_option = request.forms.get('delete')
        #0 = modify, 1 = delete

        print "the delete option is ", delete_option

        #call either delete or modify
        if int(delete_option) == 0:
            modify_local_element_in_store(element_id, entry)
        elif int(delete_option) == 1:
            delete_element_from_local_store(element_id, False)

    @app.post('/board/<element_id:int>/')
    def client_action_received(element_id):
        global board, node_id, action_queue, message_counter

        print "You receive an element"
        print "id is ", node_id
        # Get the entry from the HTTP body
        entry = request.forms.get('entry')

        delete_option = request.forms.get('delete')
        #0 = modify, 1 = delete

        print "the delete option is ", delete_option

        #call either delete or modify
        if int(delete_option) == 0:
            modify_element_in_store(element_id, entry, False)
            action_str = 'MODIFY'
            action_queue.append(Action(action_str, entry, element_id))
            update_counter(message_counter, element_id, float('inf'))

        elif int(delete_option) == 1:
            delete_element_from_store(element_id, False)
            action_str = 'DELETE'
            action_queue.append(Action(action_str, None, element_id))

    #With this function you handle requests from other nodes like add modify or delete
    @app.post('/propagate/<action>/<element_id:int>')
    def propagation_received(action, element_id):
        global my_token, local_board, message_counter, board, action_queue

        #get entry from http body
        entry = request.forms.get('entry')
        print "the action is", action

        # Handle requests
        if action == "ADD":
            add_new_element_to_store(element_id, entry, True)

        # Modify the board entry
        elif action == "MODIFY":
            modification_counter = int(request.forms.get("modification_counter"))
            print("entry: {}".format(entry))
            print("modification_counter: {}".format(modification_counter))
            if element_id not in board:
                # If we receive a modification action for a message we don't yet
                # have on this node, we add the action to a queue, (unmatched_queue)
                # and we handle it later.
                # But we need to check the message counter for the message;
                # in the case that the message has been deleted from the
                # board (the message counter is -1), we don't want to
                # add it to the unmatched_queue, since it can never get handled.
                if element_id in message_counter:
                    if message_counter[element_id] >= 0:
                        action = Action(action, entry, element_id, modification_counter)
                        unmatched_queue.append(action)
                else:
                    action = Action(action, entry, element_id, modification_counter)
                    unmatched_queue.append(action)
            else:
                if element_id in message_counter:
                    if modification_counter > message_counter[element_id]:
                        message_counter[element_id] = modification_counter
                        # Only if this modification is newer than the one we have
                        # our in store, we should update our entry:
                        modify_element_in_store(element_id, entry, True)
                else:
                    # If the message has no counter yet, we can safely modify it
                    # and set its message counter.
                    message_counter[element_id] = modification_counter
                    modify_element_in_store(element_id, entry, True)

        # Delete the entry from the board
        elif action == "DELETE":
            # If the element is not on the board and has not been deleted before we add
            # a deletion action to the unmatched queue
            if element_id not in board and element_id not in message_counter:
                action = Action(action, entry, element_id, None)
                unmatched_queue.append(action)
            # If the element exists, we delete it
            else:
                delete_element_from_store(element_id, True)

        elif action == "TOKEN":
            last_msg = int(request.forms.get("last_msg_id"))
            print("Received token, last_msg_id is: {}".format(last_msg))
            mod_c = int(request.forms.get("modification_counter"))
            my_token = {"last_msg_id": last_msg, "modification_counter": mod_c}

            if len(action_queue) == 0 and len(local_board.keys()) == 0:
                # Pass the token forward after a set amount of time if no work is queued.
                token_timer = Timer(0.1, forward_token)
                token_timer.start()
            else:
                # If we have local changes that need to be propagated to other nodes,
                # we want to:
                # 1) Add messages from local_board to the global board, ensuring
                #    no collisions with message IDs.
                # 2) Perform queued actions in the action_queue.
                # 3) Call forward_token once we finish.
                last_id = my_token["last_msg_id"]
                for local_key in local_board.keys():
                    local_msg = local_board[local_key]

                    # Transfer message from local_board to board.
                    add_new_element_to_store(last_id, local_msg)
                    # Delete it from local after transfer
                    del local_board[local_key]

                    # Propagate this message to other nodes.
                    thread = Thread(target=propagate_to_vessels,
                                    args=('/propagate/ADD/' + str(last_id),
                                          {'entry': local_msg},
                                          'POST'))
                    thread.daemon = True
                    thread.start()
                    # Ensure the next addition can use a unique message ID.
                    last_id += 1

                # Update the token with the next unique message ID.
                my_token["last_msg_id"] = last_id

                for action in action_queue:
                    print(action)
                    mod_counter = my_token["modification_counter"]
                    if action.action_str == "MODIFY":
                        update_counter(message_counter, action.msg_id, my_token["modification_counter"])
                        my_token["modification_counter"] += 1
                    thread = Thread(target=propagate_to_vessels,
                                    args=('/propagate/' + str(action.action_str) + '/' + str(action.msg_id),
                                          {'entry': action.msg,
                                           'modification_counter': mod_counter},
                                          'POST'))
                    thread.daemon = True
                    thread.start()

                # Empty the queue after we've propagated all the actions.
                action_queue = []

                forward_token()


    # ------------------------------------------------------------------------------------------------------
    # DISTRIBUTED COMMUNICATIONS FUNCTIONS
    # ------------------------------------------------------------------------------------------------------
    def contact_vessel(vessel_ip, path, payload=None, req='POST'):
        # Try to contact another server (vessel) through a POST or GET, once
        success = False
        try:
            if 'POST' in req:
                res = requests.post('http://{}{}'.format(vessel_ip, path), data=payload)
            elif 'GET' in req:
                res = requests.get('http://{}{}'.format(vessel_ip, path))
            else:
                print 'Non implemented feature!'
            print(res)
            if res.status_code == 200:
                success = True
        except Exception as e:
            print e
        return success

    def propagate_to_vessels(path, payload = None, req = 'POST'):
        global vessel_list, node_id

        for vessel_id, vessel_ip in vessel_list.items():
            if vessel_id != node_id: # don't propagate to yourself
                success = contact_vessel(vessel_ip, path, payload, req)
                if not success:
                    print "\n\nCould not contact vessel {}\n\n".format(vessel_id)


    # Token related functions
    def forward_token():
        global my_token
        print("forwarding token: {}".format(my_token))
        """Send the token to the next node in the ring."""
        if my_token is None:
            raise Exception("Node {} does not currently hold the token".format(node_id))
        else:
            print("Sending token to: Node {}".format(next_id))
            thread = Thread(target=contact_vessel,
                            args=(vessel_list[next_id],
                                  "/propagate/TOKEN/0",  # Dummy element_id to fit format
                                  {"last_msg_id": my_token["last_msg_id"],
                                   "modification_counter": my_token["modification_counter"]},
                                  "POST"))
            thread.daemon = True
            thread.start()
            my_token = None

    # ------------------------------------------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------------------------------------------
    def main():
        global vessel_list, node_id, app, my_token, next_id

        port = 80
        parser = argparse.ArgumentParser(description='Your own implementation of the distributed blackboard')
        parser.add_argument('--id', nargs='?', dest='nid', default=1, type=int, help='This server ID')
        parser.add_argument('--vessels', nargs='?', dest='nbv', default=1, type=int, help='The total number of vessels present in the system')
        args = parser.parse_args()
        node_id = args.nid
        vessel_list = dict()
        # We need to write the other vessels IP, based on the knowledge of their number
        for i in range(1, args.nbv + 1):
            vessel_list[i] = "10.1.0.{}".format(i)

        #
        # Set up token ring stuff.
        #
        node_ids = sorted(vessel_list.keys())
        if node_ids.index(node_id) == len(node_ids) - 1:
            # If we are the last node in the list, the next node will be the
            # first one in the list.
            next_id = node_ids[0]
        else:
            # Otherwise, the next node is simply the next one in the list
            next_id = node_ids[node_ids.index(node_id) + 1]
        print("Next node in token ring: {}".format(next_id))

        # The first node in the node list should be the initial token carrier.
        if node_ids[0] == node_id:
            # The token is a dictionary containing the following fields:
            # - last_msg_id: The message ID of the latest message on the global board,
            #                as far as the token bearer knows.
            # - modification_counter: This counter is needed to determine
            #                         priority when multiple nodes want
            #                         to modify the same message.
            my_token = {"last_msg_id": 1, "modification_counter": 0}
        else:
            my_token = None
        print('has token', my_token is not None)

        #
        # Start the node.
        #
        try:
            if my_token is not None:
                # The initial token carrier must start the token forwarding process.
                token_timer = Timer(10, forward_token)
                token_timer.start()

            run(app, host=vessel_list[node_id], port=port)
        except Exception as e:
            print e
    # ------------------------------------------------------------------------------------------------------
    if __name__ == '__main__':
        main()


except Exception as e:
        traceback.print_exc()
        while True:
            time.sleep(60.)
