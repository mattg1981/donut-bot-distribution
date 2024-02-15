import json
import os
import pathlib
from datetime import datetime, timedelta

import praw
import prawcore
from web3 import Web3
from urllib import request
from distribution_tasks.distribution_task import DistributionTask


class BuildCacheDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 200

    def build_and_cache_user_weights(self):
        self.logger.info("  build user weights...")
        file_exists = super().get_current_document_version("user_weights")

        if file_exists:
            self.logger.info("    ... file exists in cache already!")
            return

        # get users file
        users = super().get_current_document_version('users')

        # get onchain tips
        onchain_tips = super().get_current_document_version('onchain_tips')
        dr = super().get_current_document_version("distribution_round")[0]
        # onchain_tips = [t for t in onchain_tips if
        #                t['timestamp'] >= dr['from_date'] and t['timestamp'] <= dr['to_date']]
        onchain_tips = [t for t in onchain_tips if
                        dr['from_date'] <= t['timestamp'] <= dr['to_date']]

        # get offchain tips (we may not know which have materialized at this point, so we will calculate the
        # weight for all tippers

        self.logger.info("  grabbing offchain tips file...")
        offchain_tips = super().get_current_document_version('offchain_tips')

        # offchain_tips = json.load(
        #     request.urlopen(
        #         f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/offchain_tips/tips_round_{super().distribution_round}.json"))

        # find all tip senders and receivers
        tippers = [t['from_user'] for t in offchain_tips if t['from_user']]
        tippers.extend([t['to_user'] for t in offchain_tips if t['to_user']])
        tippers.extend([o['from_user'] for o in onchain_tips if o['from_user']])
        tippers.extend([o['to_user'] for o in onchain_tips if o['to_user']])

        tippers = list(set(tippers))

        self.logger.info("  connecting to provider - infura.io [mainnet]... ")
        eth_w3 = Web3(Web3.HTTPProvider(os.getenv('INFURA_IO_ETH')))
        if not eth_w3.is_connected():
            self.logger.error("  failed to connect to infura.io [mainnet]")
            exit(4)
        else:
            self.logger.info("  success.")

        self.logger.info("  connecting to provider - infura.io [arb1]... ")
        arb1_w3 = Web3(Web3.HTTPProvider(os.getenv('INFURA_IO_ARB1')))
        if not arb1_w3.is_connected():
            self.logger.error("  failed to connect to infura.io [arb1]")
            exit(4)
        else:
            self.logger.info("  success.")

        self.logger.info("  connecting to provider - ankr.com...")
        gno_w3 = Web3(Web3.HTTPProvider(os.getenv('ANKR_API_PROVIDER')))
        if not gno_w3.is_connected():
            self.logger.error("  failed to connect to ankr node [gnosis]")
            exit(4)
        else:
            self.logger.info("  success.")

        # lookup abi
        with open(os.path.join(pathlib.Path().resolve(), "contracts/erc20.json"), 'r') as f:
            erc20_abi = json.load(f)

        with open(os.path.join(pathlib.Path().resolve(), "contracts/staking.json"), 'r') as f:
            staking_abi = json.load(f)

        with open(os.path.join(pathlib.Path().resolve(), "contracts/uniswap_token.json"), 'r') as f:
            uniswap_abi = json.load(f)

        user_weights = []

        # contrib
        contrib_contract = gno_w3.eth.contract(address=gno_w3.to_checksum_address(
            self.config["contracts"]["gnosis"]["contrib"]), abi=erc20_abi)

        # TODO uncomment when arb1 migration complete
        # contrib_contract = arb1_w3.eth.contract(address=arb1_w3.to_checksum_address(
        #    self.config["contracts"]["arb1"]["contrib"]), abi=erc20_abi)

        # mainnet donut
        donut_eth_contract = eth_w3.eth.contract(address=eth_w3.to_checksum_address(
            self.config["contracts"]["mainnet"]["donut"]), abi=erc20_abi)

        # gnosis donut
        donut_gno_contract = gno_w3.eth.contract(address=gno_w3.to_checksum_address(
            self.config["contracts"]["gnosis"]["donut"]), abi=erc20_abi)

        # arb1 donut
        donut_arb1_contract = arb1_w3.eth.contract(address=arb1_w3.to_checksum_address(
            self.config["contracts"]["arb1"]["donut"]), abi=erc20_abi)

        # gnosis staking
        staking_gno_contract = gno_w3.eth.contract(address=gno_w3.to_checksum_address(
            self.config["contracts"]["gnosis"]["staking"]), abi=staking_abi)

        # mainnet staking
        staking_eth_contract = eth_w3.eth.contract(address=eth_w3.to_checksum_address(
            self.config["contracts"]["mainnet"]["staking"]), abi=staking_abi)

        # gnosis lp
        lp_gno_contract = gno_w3.eth.contract(address=gno_w3.to_checksum_address(
            self.config["contracts"]["gnosis"]["lp"]), abi=uniswap_abi)

        # mainnet lp
        lp_eth_contract = eth_w3.eth.contract(address=eth_w3.to_checksum_address(
            self.config["contracts"]["mainnet"]["lp"]), abi=uniswap_abi)

        # TODO add arb1 lp

        self.logger.info("  retrieving reserves and calculating multipliers...")
        was_success = False
        for j in range(1,8):
            try:
                eth_lp_supply = lp_eth_contract.functions.totalSupply().call()
                gno_lp_supply = lp_gno_contract.functions.totalSupply().call()

                # eth_staking_supply = staking_eth_contract.functions.totalSupply().call()
                # gno_staking_supply = staking_gno_contract.functions.totalSupply().call()

                uniswap_eth_donuts = lp_eth_contract.functions.getReserves().call()
                uniswap_gno_donuts = lp_gno_contract.functions.getReserves().call()

                mainnet_multiplier = uniswap_eth_donuts[0] / eth_lp_supply
                gno_multiplier = uniswap_gno_donuts[0] / gno_lp_supply

                was_success = True
                break
            except Exception as e:
                self.logger.error(e)
                continue

        if not was_success:
            self.logger.error("  unable to query at this time, attempt at a later time...")
            exit(4)

        i = 0
        for tipper in tippers:
            i = i + 1
            self.logger.info(f"  calculating weight for user: [{tipper}] ({i} / {len(tippers)})")
            user = next((u for u in users if tipper.lower() == u['username'].lower()), None)

            if not user:
                self.logger.warning(f"    user [{tipper}] not found in user file")
                continue

            address = eth_w3.to_checksum_address(user['address'])

            was_success = False
            for j in range(1,8):
                try:
                    contrib_balance = contrib_contract.functions.balanceOf(address).call()

                    eth_donut_balance = donut_eth_contract.functions.balanceOf(address).call()
                    gno_donut_balance = donut_gno_contract.functions.balanceOf(address).call()
                    arb1_donut_balance = donut_arb1_contract.functions.balanceOf(address).call()

                    staked_mainnet_balance = staking_eth_contract.functions.balanceOf(address).call() * mainnet_multiplier
                    staked_gno_balance = staking_gno_contract.functions.balanceOf(address).call() * gno_multiplier

                    donut_balance = (arb1_donut_balance +
                                     eth_donut_balance +
                                     gno_donut_balance +
                                     staked_mainnet_balance +
                                     staked_gno_balance)

                    donut_balance = arb1_w3.from_wei(donut_balance, "ether")
                    contrib_balance = arb1_w3.from_wei(contrib_balance, "ether")
                    weight = donut_balance if donut_balance < contrib_balance else contrib_balance
                except Exception as e:
                    self.logger.error(e)
                    continue

                self.logger.info(f"    donut: [{donut_balance}] - contrib [{contrib_balance}] - weight [{weight}]")

                user_weights.append({
                    'tipper': tipper,
                    'donut': int(donut_balance),
                    'contrib': int(contrib_balance),
                    'weight': int(weight)
                })

                user['contrib'] = int(contrib_balance)
                user["donut"] = int(donut_balance)
                user['weight'] = int(weight)

                was_success = True
                break

            if not was_success:
                self.logger.error("  unable to query at this time, attempt at a later time...")
                exit(4)

        fp = super().save_document_version(user_weights, "user_weights")
        super().cache_file(fp)

    def build_and_cache_ineligible_users(self):
        self.logger.info("  build ineligible users...")

        ineligible_users_filename = "ineligible_users"
        file_exists = super().get_current_document_version(ineligible_users_filename)

        if file_exists:
            self.logger.info("    ... file exists in cache already!")
            return

        distribution = super().get_current_document_version('distribution')
        distribution_round = super().get_current_document_version('distribution_round')

        distribution_round_end_date = datetime.strptime(distribution_round[0]['to_date'], '%Y-%m-%d %H:%M:%S.%f')
        cutoff_date = distribution_round_end_date - timedelta(days=60)

        # creating an authorized reddit instance
        reddit = praw.Reddit(client_id=os.getenv('INELIGIBLE_CLIENT_ID'),
                             client_secret=os.getenv('INELIGIBLE_CLIENT_SECRET'),
                             user_agent="ethtrader ineligible-users (by u/mattg1981)")

        reddit.read_only = True

        ineligible_users = []

        idx = 0
        for d in distribution:
            idx += 1
            self.logger.info(
                f"  checking eligiblity requirements for {d['username']} [{idx} of {len(distribution)}]")
            redditor = reddit.redditor(d['username'])

            try:
                if hasattr(redditor, 'is_suspended'):
                    if redditor.is_suspended:
                        self.logger.info(f"    adding user [{d['username']}] to ineligible list: user is suspended")
                        ineligible_users.append({
                            'user': d['username'],
                            'reason': 'suspended'
                        })
                        continue

                if redditor.total_karma < 100:
                    self.logger.info(f"    adding user [{d['username']}] to ineligible list: karma < 100")
                    ineligible_users.append({
                        'user': d['username'],
                        'reason': 'karma'
                    })
                    continue

                if datetime.fromtimestamp(redditor.created) > cutoff_date:
                    self.logger.info(f"    adding user [{d['username']}] to ineligible list: created < 60 days")
                    ineligible_users.append({
                        'user': d['username'],
                        'reason': 'age'
                    })
                    continue

                self.logger.info("    ok...")

            except prawcore.exceptions.NotFound as e:
                self.logger.info(
                    f"    removing user [{d['username']}] from distribution: user is deleted (not found)")
                self.logger.error(e)

                ineligible_users.append({
                    'user': d['username'],
                    'reason': 'deleted or shadow ban'
                })
                continue

            except Exception as e:
                self.logger.error(e)

        path = super().save_document_version(ineligible_users, ineligible_users_filename)
        super().cache_file(path)

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        self.build_and_cache_user_weights()
        self.build_and_cache_ineligible_users()

        if pipeline_config['build-cache']:
            self.logger.info("BUILD-CACHE directive ... aborting run here...")
            exit(0)

        return super().update_pipeline(pipeline_config, {
            "user_weights": "user_weights",
            "ineligible_users": "ineligible_users"
        })
