import os
import urllib
import numpy as np
import pandas as pd

from distribution_tasks.distribution_task import DistributionTask


class CalculateTipsBonusDistributionTask(DistributionTask):
    DONUTS_FROM_TIPPING_OTHERS = 170000
    DONUTS_FROM_RECEIVING_TIPS = 340000
    GOV_WEIGHT_THRESHOLD = 500

    # REGISTERED_USERS_URL = "https://ethtrader.github.io/donut.distribution/users.json"
    # ONCHAIN_TIPS_FILE = "https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/onchain_tips/onchain_tips.csv"
    # OFFCHAIN_TIPS_URL = "https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/offchain_tips/materialized/round_#ROUND#_materialized_tips.json"

    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 600

    def get_onchain_post_tips(self):
        """ Get onchain tips from the csv file and filter out tips outside the range or comment tips """
        self.logger.info("  pulling down onchain tips file...")
        oc_tips = super().get_current_document_version('onchain_tips')
        # onchain_tips = pd.read_csv(self.ONCHAIN_TIPS_FILE)
        onchain_tips = pd.DataFrame.from_records(oc_tips).astype({'amount': 'float'})

        dr = super().get_current_document_version("distribution_round")[0]

        onchain_tips = onchain_tips[(onchain_tips['timestamp'] >= dr['from_date']) & (onchain_tips['timestamp'] <= dr['to_date'])]
        # onchain_tips = onchain_tips[(self.START_BLOCK <= onchain_tips["block"]) & (onchain_tips["block"] <= self.END_BLOCK)]

        onchain_tips = onchain_tips[~onchain_tips["content_id"].astype(str).str.startswith("t1")]
        onchain_tips["type"] = "onchain"

        return onchain_tips

    # def get_offchain_post_tips(self, offchain_tips_url):
    def get_offchain_post_tips(self):
        """ Get offchain tips from url and filter out comment tips """
        try:
            self.logger.info("  pulling down offchain tips file...")
            # offchain_tips = pd.read_json(offchain_tips_url)
            materialized_tips = super().get_current_document_version('materialized_tips')
            offchain_tips = pd.DataFrame.from_records(materialized_tips).astype({'amount': 'float'})
        except urllib.error.HTTPError:
            print("No offchain tips found or url is not valid")
            return pd.DataFrame(columns=["from_user", "to_user", "amount", "type"])

        offchain_tips = offchain_tips[offchain_tips["token"] == "donut"]
        offchain_tips = offchain_tips[~offchain_tips["parent_content_id"].str.startswith("t1")]
        offchain_tips = offchain_tips[["from_user", "to_user", "amount"]]
        offchain_tips["type"] = "offchain"

        return offchain_tips

    # def get_post_tips(self, offchain_tips_url):
    def get_post_tips(self):
        """ Get both onchain and offchain tips and merge them together """
        onchain_tips = self.get_onchain_post_tips()
        # offchain_tips = self.get_offchain_post_tips(offchain_tips_url)
        offchain_tips = self.get_offchain_post_tips()

        if offchain_tips.empty:
            return onchain_tips

        return pd.concat([onchain_tips, offchain_tips])

    def add_weights(self, tips):
        """ Add the weights of sender and receiver to the tips dataframe """
        users = super().get_current_document_version('users')
        # users_weight = pd.read_json(self.REGISTERED_USERS_URL)

        users_weight = pd.DataFrame.from_records(users)

        tips_with_weight = pd.merge(tips, users_weight, how="inner", left_on="from_user", right_on="username")
        tips_with_weight = tips_with_weight[["from_user", "to_user", "amount", "weight"]].rename(
            columns={"weight": "sender_weight"}).astype({"sender_weight": "int32"})

        # Adding the weight of the receiver also removes tips to users that are not registered
        tips_with_weight = pd.merge(tips_with_weight, users_weight, how="inner", left_on="to_user", right_on="username")
        tips_with_weight = tips_with_weight[["from_user", "to_user", "amount", "sender_weight", "weight"]].rename(
            columns={"weight": "receiver_weight"}).astype({"receiver_weight": "int32"})

        return tips_with_weight

    def filter_out_by_gov_weight(self, tips):
        """ Filter out tips from users with a weight below the threshold """
        return tips[tips["sender_weight"] > self.GOV_WEIGHT_THRESHOLD]

    def compute_offchain_tips(self, post_tips):
        """ Sum up the offchain tips """
        offchain_tips = post_tips[post_tips["type"] == "offchain"]
        offchain_tips_sent = offchain_tips.groupby("from_user")["amount"].sum().sort_values(ascending=False)
        offchain_tips_sent = offchain_tips_sent.reset_index().rename(
            columns={"from_user": "user", "amount": "offchain_tips_sent"})

        offchain_tips_received = offchain_tips.groupby("to_user")["amount"].sum().sort_values(ascending=False)
        offchain_tips_received = offchain_tips_received.reset_index().rename(
            columns={"to_user": "user", "amount": "offchain_tips_received"})

        offchain_tips = pd.merge(offchain_tips_sent, offchain_tips_received, how="outer", on="user").fillna(0)

        return offchain_tips[["user", "offchain_tips_sent", "offchain_tips_received"]]

    def compute_donuts_from_tips(self, post_tips):
        """ Compute donuts from sending tips """
        count_tips = post_tips.groupby("from_user").count().sort_values(by="amount", ascending=False)
        count_tips = count_tips.reset_index()[["from_user", "amount"]].rename(
            columns={"from_user": "user", "amount": "num_post_tips"})
        count_tips["percentage_tips"] = count_tips["num_post_tips"] / count_tips["num_post_tips"].sum()
        count_tips["donut_upvoter"] = count_tips["percentage_tips"] * self.DONUTS_FROM_TIPPING_OTHERS

        return count_tips[["user", "donut_upvoter"]]

    def compute_donuts_from_quad_rank(self, post_tips):
        """ Compute donuts from receiving tips """
        post_tips["quad_rank_points"] = post_tips["sender_weight"] ** 0.5
        count_quad_rank = post_tips.groupby("to_user")["quad_rank_points"].sum().sort_values(ascending=False)
        count_quad_rank = count_quad_rank.reset_index().rename(
            columns={"to_user": "user", "quad_rank_points": "quad_rank_points"})
        count_quad_rank["percentage_quad_rank"] = count_quad_rank["quad_rank_points"] / count_quad_rank[
            "quad_rank_points"].sum()
        count_quad_rank["quad_rank"] = count_quad_rank["percentage_quad_rank"] * self.DONUTS_FROM_RECEIVING_TIPS

        return count_quad_rank[["user", "quad_rank"]]

    def create_distribution(self, count_tips, count_quad_rank, offchain_tips):
        """ Create the distribution dataframe """
        distribution = pd.merge(count_tips, count_quad_rank, how="outer", on="user").fillna(0)
        distribution = pd.merge(distribution, offchain_tips, how="outer", on="user").fillna(0)

        distribution["donut_upvoter"] = np.round(distribution["donut_upvoter"], 0).astype(int)
        distribution["quad_rank"] = np.round(distribution["quad_rank"], 0).astype(int)
        distribution["total_donuts"] = (distribution["donut_upvoter"] + distribution["quad_rank"] +
                                        distribution["offchain_tips_received"] - distribution["offchain_tips_sent"])
        distribution = distribution.sort_values(by="total_donuts", ascending=False)
        return distribution

    def process(self, pc):
        super().process(pc)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        # offchain_tips = self.OFFCHAIN_TIPS_URL.replace("#ROUND#", str(super().distribution_round))
        # post_tips = self.get_post_tips(offchain_tips)
        post_tips = self.get_post_tips()
        offchain_tips = self.compute_offchain_tips(post_tips)

        post_tips_with_weight = self.filter_out_by_gov_weight(self.add_weights(post_tips))
        count_tips = self.compute_donuts_from_tips(post_tips_with_weight)
        count_quad_rank = self.compute_donuts_from_quad_rank(post_tips_with_weight)

        distribution = self.create_distribution(count_tips, count_quad_rank, offchain_tips)

        output = [
            {
                "username": row["user"],
                "donut_upvoter": int(row["donut_upvoter"]),
                "quad_rank": int(row["quad_rank"]),
                "points": int(row["donut_upvoter"]) + int(row["quad_rank"])
            }
            for _, row in distribution.iterrows()
        ]

        super().save_document_version(output, 'tipping_bonus')

        return super().update_pipeline(pc, {
            'tipping_bonus': 'tipping_bonus',
        })
