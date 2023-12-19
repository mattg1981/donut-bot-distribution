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

    awards = [
        {
            "to": "mattg1981",
            "donut": 100000,
            "contrib": 20000,
            "reason": "donut initiative"
        },
        {
            "to": "reddito321",
            "donut": 150000,
            "contrib": 150000,
            "reason": "donut initiative"
        },
        {
            "to": "mattg1981",
            "donut": 10000,
            "contrib": 2000,
            "reason": "reward organizer"
        }
    ]

    # get users file that will be used for any user <-> address lookups
    users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))

    # map awardee addresses
    for award in awards:
        user = next((u for u in users if u['username'].lower() == award['to'].lower()), None)

        if not user:
            print(f"donut reward user not found, ensure you typed the name correctly: [{award['to']}]")
            exit(4)

        award["address"] = user["address"]

    # encode the donut data
    distribute_contract_data = distribute_contract.encodeABI("distribute", [
        [w3.to_checksum_address(a['address']) for a in awards if float(a['donut']) > 0],
        [w3.to_wei(a['donut'], 'ether') for a in awards if float(a['donut']) > 0],
        w3.to_checksum_address(GNO_DONUT_CONTRACT_ADDRESS)
    ])

    # encode the contrib data
    contrib_contract_data = gno_contrib_contract.encodeABI("mintMany", [
        [w3.to_checksum_address(a['address']) for a in awards if float(a['contrib']) > 0],
        [w3.to_wei(a['contrib'], 'ether') for a in awards if float(a['contrib']) > 0]
    ])

    # create the transactions to be passed into the safe tx builder helper
    transactions = [
        SafeTx(to=w3.to_checksum_address(GNO_DISTRIBUTE_CONTRACT_ADDRESS), data=distribute_contract_data),
        SafeTx(to=w3.to_checksum_address(GNO_CONTRIB_CONTRACT_ADDRESS), data=contrib_contract_data),
    ]

    # build the tx builder json
    tx = build_tx_builder_json(f"donut-initiative awards", transactions)

    # save the tx builder json to file to be uploaded
    output_location = f"../out/ad_hoc/donut-initiative_{datetime.now().strftime('%m-%d-%Y')}.json"

    if os.path.exists(output_location):
        os.remove(output_location)

    with open(os.path.normpath(output_location), 'w') as f:
        json.dump(tx, f, indent=4)
