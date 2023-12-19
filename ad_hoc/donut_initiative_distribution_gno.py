import json
import os
from datetime import datetime
from urllib import request

from web3 import Web3

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from safe.safe_tx import SafeTx
from safe.safe_tx_builder import build_tx_builder_json

"""
    This file is largely the same as Task_1300_Build_Safe_Transaction except the transactions are hand-crafted
    per the donut-initiative rewards
"""

GNO_DISTRIBUTE_CONTRACT_ADDRESS = "0xea0B2A9a711a8Cf9F9cAddA0a8996c0EDdC44B37"
GNO_CONTRIB_CONTRACT_ADDRESS = "0xFc24F552fa4f7809a32Ce6EE07C09Dcd7A41988F"
GNO_DONUT_CONTRACT_ADDRESS = "0x524b969793a64a602342d89bc2789d43a016b13a"

if __name__ == '__main__':

    with open(os.path.normpath("../contracts/gno_distribute_abi.json"), 'r') as f:
        gno_distribute_abi = json.load(f)

    with open(os.path.normpath("../contracts/gno_contrib_abi.json"), 'r') as f:
        gno_contrib_abi = json.load(f)

        w3 = Web3()
        distribute_contract = w3.eth.contract(address=w3.to_checksum_address(GNO_DISTRIBUTE_CONTRACT_ADDRESS),
                                              abi=gno_distribute_abi)

        gno_contrib_contract = w3.eth.contract(address=w3.to_checksum_address(GNO_CONTRIB_CONTRACT_ADDRESS),
                                               abi=gno_contrib_abi)

        # this is for the following two initiatives:
        # 1 - https://www.reddit.com/r/ethtrader/comments/16sq29i/donut_initiative_improve_mydonuts_karmadonut/
        # 2 - https://www.reddit.com/r/ethtrader/comments/17tjmmu/donut_initiative_rewarding_the_development_of/

        donuts_awarded = [
            {
                "to": "mattg1981",
                "amount": 100000,
                "reason": "donut initiative",
                "address": None
            },
            {
                "to": "reddito321",
                "amount": 150000,
                "reason": "donut initiative",
                "address": None
            },
            {
                "to": "mattg1981",
                "amount": 10000,
                "reason": "reward organizer",
                "address": None
            }
        ]

        contrib_awarded = [
            {
                "to": "mattg1981",
                "amount": 20000,
                "reason": "donut initiative",
                "address": None
            },
            {
                "to": "reddito321",
                "amount": 150000,
                "reason": "donut initiative",
                "address": None
            },
            {
                "to": "mattg1981",
                "amount": 2000,
                "reason": "reward organizer",
                "address": None
            }
        ]

        # get users file that will be used for any user <-> address lookups
        users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))

        # map donut awardee addresses
        for d in donuts_awarded:
            user = next((u for u in users if u['username'].lower() == d['to'].lower()),None)

            if not user:
                print(f"donut reward user not found, ensure you typed the name correctly: [{d['to']}]")

            d["address"] = user["address"]

        # map contrib awardee addresses
        for c in contrib_awarded:
            user = next((u for u in users if u['username'].lower() == c['to'].lower()),None)

            if not user:
                print(f"contrib user not found, ensure you typed the name correctly: [{d['to']}]")

            c["address"] = user["address"]

        # encode the donut data
        distribute_contract_data = distribute_contract.encodeABI("distribute", [
                    [w3.to_checksum_address(d['address']) for d in donuts_awarded if float(d['amount']) > 0],
                    [w3.to_wei(d['amount'], 'ether') for d in donuts_awarded if float(d['amount']) > 0],
                    w3.to_checksum_address(GNO_DONUT_CONTRACT_ADDRESS)
                ])

        # encode the contrib data
        contrib_contract_data = gno_contrib_contract.encodeABI("mintMany", [
                [w3.to_checksum_address(c['address']) for c in contrib_awarded if float(c['amount']) > 0],
                [w3.to_wei(c['amount'], 'ether') for c in contrib_awarded if float(c['amount']) > 0]
            ])

        transactions = [
            SafeTx(to=w3.to_checksum_address(GNO_DISTRIBUTE_CONTRACT_ADDRESS), value=0, data=distribute_contract_data),
            SafeTx(to=w3.to_checksum_address(GNO_CONTRIB_CONTRACT_ADDRESS), value=0, data=contrib_contract_data),
        ]

        # build the tx builder json
        tx = build_tx_builder_json(f"donut-initiative awards", transactions)

        datetime_string = datetime.now().strftime('%m-%d-%Y')

        # save the tx builder json to file to be uploaded
        output_location = f"../out/ad_hoc/donut-initiative_{datetime_string}.json"

        if os.path.exists(output_location):
            os.remove(output_location)

        with open(os.path.normpath(output_location), 'w') as f:
            json.dump(tx, f, indent=4)




