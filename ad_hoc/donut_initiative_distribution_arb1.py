import json
import os
from datetime import datetime
from urllib import request

from web3 import Web3

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from safe.safe_tx import SafeTx
from safe.safe_tx_builder import build_tx_builder_json, SafeChain

"""
    This file is largely the same as Task_1300_Build_Safe_Transaction except the transactions are hand-crafted
    per the donut-initiative rewards
"""

ARB1_DISTRIBUTE_CONTRACT_ADDRESS = "0xf4d6a6585BDaebB6456050Ae456Bc69ea7f51838"
ARB1_CONTRIB_CONTRACT_ADDRESS = "0xF28831db80a616dc33A5869f6F689F54ADd5b74C"
ARB1_DONUT_CONTRACT_ADDRESS = "0xF42e2B8bc2aF8B110b65be98dB1321B1ab8D44f5"

if __name__ == '__main__':

    with open(os.path.normpath("../contracts/distribute_abi.json"), 'r') as f:
        distribute_abi = json.load(f)

    with open(os.path.normpath("../contracts/arb1_contrib_abi.json"), 'r') as f:
        arb1_contrib_abi = json.load(f)

    w3 = Web3()
    distribute_contract = w3.eth.contract(address=w3.to_checksum_address(ARB1_DISTRIBUTE_CONTRACT_ADDRESS),
                                          abi=distribute_abi)

    gno_contrib_contract = w3.eth.contract(address=w3.to_checksum_address(ARB1_CONTRIB_CONTRACT_ADDRESS),
                                           abi=arb1_contrib_abi)

    awards = [
        {
            "user": "u/DBRiMatt",
            "donut": 10_000,
            "contrib": 10_000,
            "reason": "round 142 contests"
        },
        {
            "user": "u/Buzzalu",
            "donut": 7_000,
            "contrib": 7_000,
            "reason": "round 142 contests"
        },
        {
            "user": "u/whodontloveboobs",
            "donut": 6500,
            "contrib": 6500,
            "reason": "round 142 contests"
        },
        {
            "user": "u/Kirtash93",
            "donut": 6000,
            "contrib": 6000,
            "reason": "round 142 contests"
        },
        {
            "user": "u/inclinedumbbellpress",
            "donut": 3000,
            "contrib": 3000,
            "reason": "round 142 contests"
        },
        {
            "user": "u/LegendRXL",
            "donut": 2000,
            "contrib": 2000,
            "reason": "round 142 contests"
        },
        {
            "user": "u/Odd-Radio-8500",
            "donut": 1000,
            "contrib": 1000,
            "reason": "round 142 contests"
        },
        {
            "user": "u/goldyluckinblokchain",
            "donut": 500,
            "contrib": 500,
            "reason": "round 142 contests"
        },
        {
            "user": "u/Mixdealyn",
            "donut": 500,
            "contrib": 500,
            "reason": "round 142 contests"
        },
        {
            "user": "u/Garden_Aria",
            "donut": 500,
            "contrib": 500,
            "reason": "round 142 contests"
        },
        {
            "user": "u/King__Robbo",
            "donut": 500,
            "contrib": 500,
            "reason": "round 142 contests"
        }
    ]

    # per https://www.reddit.com/r/ethtrader/comments/1fa8if9/governance_poll_proposal_automatic_surcharge_on/
    total_donut_awards = sum(a["donut"] for a in awards)
    total_contrib_awards = sum(a["contrib"] for a in awards)

    organizer_donut_reward = int(max(.025 * total_donut_awards, 2500))
    organizer_contrib_reward = int(max(.025 * total_contrib_awards, 2500))

    awards.append({
        "user": "mattg1981",
        "donut": organizer_donut_reward,
        "contrib": organizer_contrib_reward,
        "reason": "organizer"
    })

    # get users file that will be used for any user <-> address lookups
    users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))

    # map awardee addresses
    for award in awards:
        if not 'address' in award:
            username = award['user'].strip().lower()
            if username.startswith("u/"):
                username = username.replace("u/", "")

            user = next((u for u in users if u['username'].lower() == username), None)

            if not user:
                print(f"user not found, ensure you typed the name correctly: [{username}]")
                exit(4)

            award["address"] = user["address"]

    # encode the donut data
    distribute_contract_data = distribute_contract.encodeABI("distribute", [
        [w3.to_checksum_address(a['address']) for a in awards if float(a['donut']) > 0],
        [w3.to_wei(a['donut'], 'ether') for a in awards if float(a['donut']) > 0],
        w3.to_checksum_address(ARB1_DONUT_CONTRACT_ADDRESS)
    ])

    # encode the contrib data
    contrib_contract_data = gno_contrib_contract.encodeABI("mintMany", [
        [w3.to_checksum_address(a['address']) for a in awards if float(a['contrib']) > 0],
        [w3.to_wei(a['contrib'], 'ether') for a in awards if float(a['contrib']) > 0]
    ])

    # create the transactions to be passed into the safe tx builder helper
    transactions = [
        SafeTx(to=w3.to_checksum_address(ARB1_DISTRIBUTE_CONTRACT_ADDRESS), data=distribute_contract_data),
        SafeTx(to=w3.to_checksum_address(ARB1_CONTRIB_CONTRACT_ADDRESS), data=contrib_contract_data),
    ]

    # build the tx builder json
    tx = build_tx_builder_json(SafeChain.ARB1, f"donut-initiative awards", transactions)

    # save the tx builder json to file to be uploaded
    output_location = f"donut-initiative_arb1_{datetime.now().strftime('%m-%d-%Y')}.json"

    if os.path.exists(output_location):
        os.remove(output_location)

    with open(output_location, 'w', newline='', encoding='utf-8') as f:
        json.dump(tx, f, indent=4, ensure_ascii=False)
