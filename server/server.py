# coding=utf-8
# ------------------------------------------------------------------------------------------------------
# TDA596 - Lab 4
#
# This module models the byzantine agreement, with the following assumptions:
# - There is only a single byzantine node
# - The byzantine node must be the first node in the list of nodes,
#   i.e. the node with IP "10.1.0.1".
# - Since we presume that the first node is the byzantine node, it is not required to assign
#   it as such i.e don't have to press the button "Byzantine" (whereas the other nodes
#   need to explicitly be assigned attack/retreat)
#
# ------------------------------------------------------------------------------------------------------
import traceback
import sys
import time
import json
import argparse
from threading import Thread

from bottle import Bottle, run, request, template
import requests

from byzantine_behavior import *
# ------------------------------------------------------------------------------------------------------
try:
    global node_id

    app = Bottle()

    final_result = ""
    final_result_vector = ""
    is_honest = None  # Non-byzantine nodes will set this to True.
    my_vote = None  # True for Attack, False for Retreat.

    # Dictionary from node IDs to their vote, + a special key "sender",
    # indicating who the original owner of the dictionary was.
    received_votes = {}

    # List to store what votes other nodes received.
    others_received_votes = list()

    # ------------------------------------------------------------------------------------------------------
    # ROUTES
    # ------------------------------------------------------------------------------------------------------
    # a single example (index) for get, and one for post
    # ------------------------------------------------------------------------------------------------------

    #No need to modify this
    @app.route('/')
    def index():
        global node_id
        return template('server/index.tpl', board_title='Vessel {}'.format(node_id),
                        members_name_string='Julio, Mihkel, Thomas')

    #------------------------------------------------------------------------------------------------------

    @app.get('/vote/result')
    def get_results():
        global final_result, final_result_vector
        """Get the results of the vote after the Byzantine agreement."""
        try:
            if final_result == "" or final_result_vector == "":
                return ""
            else:
                return "The decision is {} and the result vector is {}".format(final_result, final_result_vector)
        except Exception as e:
            print(e)
        return False

    @app.post('/vote/attack')
    def attack():
        global my_vote, is_honest, received_votes
        try:
            my_vote = True
            is_honest = True
            received_votes[str(node_id)] = my_vote
            thread = Thread(target=propagate_to_vessels,
                            args=('/propagate/vote',
                            {'vote': my_vote,
                            'sender': node_id},
                            'POST'))
            thread.daemon = True
            thread.start()
            return True
        except Exception as e:
            print(e)
        return False

    @app.post('/vote/retreat')
    def retreat():
        global my_vote, is_honest, received_votes
        try:
            my_vote = False
            is_honest = True
            received_votes[str(node_id)] = my_vote
            thread = Thread(target=propagate_to_vessels,
                            args=('/propagate/vote',
                            {'vote': my_vote,
                            'sender': node_id},
                            'POST'))
            thread.daemon = True
            thread.start()
            return True
        except Exception as e:
            print(e)
        return False

    @app.post('/vote/byzantine')
    def byzantine():
        global is_honest, received_votes
        try:
            print('POST /vote/byzantine')
            is_honest = False
            return True
        except Exception as e:
            print(e)
        return False


    @app.post('/propagate/vote')
    def propagation_received_vote():
        """Receive the vote of another node (attack/retreat)."""
        global received_votes, is_honest

        vote = request.forms.get('vote')
        sender = request.forms.get('sender')
        received_votes[sender] = vote

        if is_honest and len(received_votes) == len(vessel_list):
            # Honest node has gotten all votes from phase 1, and propagates their
            # (honest) votes to the other nodes.
            thread = Thread(target=propagate_to_vessels,
                            args=('/propagate/results',
                            received_votes,
                            'POST'))
            thread.daemon = True
            thread.start()

        elif not is_honest and len(received_votes) + 1 == len(vessel_list):
            # Node is byzantine and has received all honest votes (we assume that
            # in all tasks there is only 1 byzantine node).
            # The byzantine node will run this once it has received all honest votes,
            # it will now send out its own byzantine votes to all other nodes.

            # Byzantine node chooses some arbitrary value for "its own" vote
            received_votes[str(node_id)] = "True"

            # Use provided function for generating byzantine votes, and send them
            # to all other nodes.
            my_votes = compute_byzantine_vote_round1(len(received_votes) - 1, len(vessel_list), True)
            for vote, ip in zip(my_votes, sorted(vessel_list.keys())):
                thread = Thread(target=contact_vessel,
                            args=(vessel_list[str(int(ip) + 1)], '/propagate/vote',
                            {'vote': vote,
                            'sender': node_id},
                            'POST'))
                thread.daemon = True
                thread.start()

            # Since there is only one byzantine node, round 2 starts right after it sends out
            # its own votes. Generates the vote vectors to send out using the given function.

            vote_lists = compute_byzantine_vote_round2(len(received_votes) - 1, len(vessel_list), True)
            for vote_list, ip in zip(vote_lists, sorted(vessel_list.keys())):
                fake_votes = dict()
                # For indexing in the fake_votes dictionary, we assume that the
                # byzantine node is the first one in the vessel_list (honest nodes
                # are nodes "after it").
                for vote_i in range(len(vote_list)):
                    fake_votes[str(vote_i + 1)] = vote_list[vote_i]
                thread = Thread(target=contact_vessel,
                            args=(vessel_list[str(int(ip) + 1)], '/propagate/results',
                            fake_votes,
                            'POST'))
                thread.daemon = True
                thread.start()


    @app.post('/propagate/results')
    def propagation_received_results():
        """Receive another node's received results (a dictionary)."""
        global others_received_votes, final_result, final_result_vector
        temp_votes_dict = {}
        for node in vessel_list:
            temp_votes_dict[node] = request.forms.get(str(node))

        others_received_votes.append(temp_votes_dict)

        if len(others_received_votes) + 1 == len(vessel_list):
            # Node now knows what votes all other nodes received. We
            # can now create the result vector.

            # Add our own received votes
            others_received_votes.append(received_votes)
            result_vector = []
            for index in range(len(others_received_votes[0])):
                # Each iteration of this loop computes one index of the result vector.

                votes_for = 0
                votes_against = 0
                for dic in others_received_votes:
                    node_vote = dic[str(index + 1)]
                    if node_vote == "True":
                        votes_for += 1
                    elif node_vote == "False":
                        votes_against += 1

                if votes_against == votes_for:
                    # In the case no value has a majority, the result vector has the
                    # value "unknown".
                    result_vector.append("unknown")
                else:
                    # Otherwise, the value with a majority gets put into the result
                    # vector.
                    result_vector.append(votes_for > votes_against)

            # Count the votes in the result vector.
            end_result = 0
            for res in result_vector:
                if res == True:
                    end_result += 1
                elif res == False:
                    end_result += -1

            end_result = end_result >= 0

            print("Result vector for node {} is: {}".format(node_id, result_vector))
            attack_str = "ATTACK" if end_result else "RETREAT"
            print("CHOSEN DECISION OF NODE {} IS: {}".format(node_id, attack_str))
            final_result = attack_str
            final_result_vector = result_vector

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
            # result is in res.text or res.json()
            print(res.text)
            if res.status_code == 200:
                success = True
        except Exception as e:
            print e
        return success

    def propagate_to_vessels(path, payload=None, req='POST'):
        global vessel_list, node_id

        for vessel_id, vessel_ip in vessel_list.items():
            if int(vessel_id) != node_id: # don't propagate to yourself
                success = contact_vessel(vessel_ip, path, payload, req)
                if not success:
                    print "\n\nCould not contact vessel {}\n\n".format(vessel_id)


    # ------------------------------------------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------------------------------------------
    def main():
        global vessel_list, node_id, app

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

        print("Number of nodes: {}".format(len(vessel_list)))

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
