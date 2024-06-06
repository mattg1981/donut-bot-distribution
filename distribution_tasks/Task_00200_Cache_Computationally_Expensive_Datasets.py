import json
import os
import pathlib
import praw
import prawcore

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from web3 import Web3
from distribution_tasks.distribution_task import DistributionTask
from datetime import datetime, timedelta

TICK_BASE = 1.0001


def tick_to_price(tick):
    return TICK_BASE ** tick


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

        # get arb 1 liqudity file
        liquidity = super().get_current_document_version('liquidity')

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

        # with open(os.path.join(pathlib.Path().resolve(), "contracts/arb1_NonfungiblePositionManager_abi.json"),
        #           'r') as f:
        #     sushi_v3_abi = json.load(f)
        #
        # with open(os.path.join(pathlib.Path().resolve(), "contracts/arb1_sushi_pool_abi.json"), 'r') as f:
        #     sushi_lp_pool_abi = json.load(f)

        user_weights = []

        # contrib
        # contrib_contract = gno_w3.eth.contract(address=gno_w3.to_checksum_address(
        #     self.config["contracts"]["gnosis"]["contrib"]), abi=erc20_abi)

        contrib_contract = arb1_w3.eth.contract(address=arb1_w3.to_checksum_address(
            self.config["contracts"]["arb1"]["contrib"]), abi=erc20_abi)

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

        self.logger.info("  retrieving reserves and calculating multipliers...")
        was_success = False
        for j in range(1, 8):
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

        # sushi_lp = []
        #
        # # get sushi lp position holders
        # position_query = """query get_positions($pool_id: ID!) {
        #   positions(where: {pool: $pool_id}) {
        #     id
        #     owner
        #     liquidity
        #     tickLower { tickIdx }
        #     tickUpper { tickIdx }
        #     pool { id }
        #     token0 {
        #       symbol
        #       decimals
        #     }
        #     token1 {
        #       symbol
        #       decimals
        #     }
        #   }
        # }"""
        #
        # # return the tick and the sqrt of the current price
        # pool_query = """query get_pools($pool_id: ID!) {
        #   pools(where: {id: $pool_id}) {
        #     tick
        #     sqrtPrice
        #   }
        # }"""
        #
        # client = Client(
        #     transport=RequestsHTTPTransport(
        #         url='https://api.thegraph.com/subgraphs/name/sushi-v3/v3-arbitrum',
        #         verify=True,
        #         retries=5,
        #     ))
        #
        # variables = {"pool_id": self.config["contracts"]["arb1"]["sushi_pool"]}
        #
        # # get pool info for current price
        # response = client.execute(gql(pool_query), variable_values=variables)
        #
        # if len(response['pools']) == 0:
        #     print("position not found")
        #     exit(-1)
        #
        # pool = response['pools'][0]
        # current_tick = int(pool["tick"])
        # current_sqrt_price = int(pool["sqrtPrice"]) / (2 ** 96)
        #
        # # get position info in pool
        # response = client.execute(gql(position_query), variable_values=variables)
        #
        # if len(response['positions']) == 0:
        #     print("position not found")
        #     exit(-1)
        #
        # for position in response['positions']:
        #     liquidity = int(position["liquidity"])
        #     tick_lower = int(position["tickLower"]["tickIdx"])
        #     tick_upper = int(position["tickUpper"]["tickIdx"])
        #     pool_id = position["pool"]["id"]
        #
        #     token0 = position["token0"]["symbol"]
        #     token1 = position["token1"]["symbol"]
        #     decimals0 = int(position["token0"]["decimals"])
        #     decimals1 = int(position["token1"]["decimals"])
        #
        #     # Compute and print the current price
        #     current_price = tick_to_price(current_tick)
        #     adjusted_current_price = current_price / (10 ** (decimals1 - decimals0))
        #
        #     sa = tick_to_price(tick_lower / 2)
        #     sb = tick_to_price(tick_upper / 2)
        #
        #     if tick_upper <= current_tick:
        #         # Only token1 locked
        #         amount0 = 0
        #         amount1 = liquidity * (sb - sa)
        #     elif tick_lower < current_tick < tick_upper:
        #         # Both tokens present
        #         amount0 = liquidity * (sb - current_sqrt_price) / (current_sqrt_price * sb)
        #         amount1 = liquidity * (current_sqrt_price - sa)
        #     else:
        #         # Only token0 locked
        #         amount0 = liquidity * (sb - sa) / (sa * sb)
        #         amount1 = 0
        #
        #     adjusted_amount0 = amount0 # / (10 ** decimals0)
        #     adjusted_amount1 = amount1 # / (10 ** decimals1)
        #
        #     sushi_lp.append({
        #         "id": position["id"],
        #         "owner": position["owner"],
        #         "tokens": adjusted_amount1
        #     })

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
            for j in range(1, 8):
                try:
                    contrib_balance = contrib_contract.functions.balanceOf(address).call()

                    eth_donut_balance = donut_eth_contract.functions.balanceOf(address).call()
                    gno_donut_balance = donut_gno_contract.functions.balanceOf(address).call()
                    arb1_donut_balance = donut_arb1_contract.functions.balanceOf(address).call()

                    staked_mainnet_balance = staking_eth_contract.functions.balanceOf(
                        address).call() * mainnet_multiplier
                    staked_gno_balance = staking_gno_contract.functions.balanceOf(address).call() * gno_multiplier

                    # sushi_lp_donuts = sum([int(s["tokens"]) for s in sushi_lp if s["owner"].lower() == address.lower()])
                    sushi_lp_donuts = sum([int(l['donut_in_lp']) for l in liquidity if l['owner'].lower() == address.lower()])

                    donut_balance = (arb1_donut_balance +
                                     eth_donut_balance +
                                     gno_donut_balance +
                                     staked_mainnet_balance +
                                     staked_gno_balance +
                                     sushi_lp_donuts)

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
                    'weight': int(weight),
                    'eth_stake': int(arb1_w3.from_wei(staked_mainnet_balance, "ether")),
                    'gnosis_stake': int(arb1_w3.from_wei(staked_gno_balance, "ether")),
                    'arb1_sushi': int(arb1_w3.from_wei(sushi_lp_donuts, "ether"))
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

        # gov weight is updated daily now so we no longer need to calculate their weight, we can use
        # the users.json file for updated weights
        # self.build_and_cache_user_weights()

        self.build_and_cache_ineligible_users()

        if pipeline_config['build-cache']:
            self.logger.info("BUILD-CACHE directive ... aborting run here...")
            exit(0)

        return super().update_pipeline(pipeline_config, {
           # "user_weights": "user_weights",
            "ineligible_users": "ineligible_users"
        })
