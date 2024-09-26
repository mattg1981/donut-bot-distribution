import csv
import glob
import os
import pathlib
import zipfile

import praw

from distribution_tasks.distribution_task import DistributionTask


class AllowSpecialMembersIfApplicableDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 150

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        onchain_tips = super().get_current_document_version(pipeline_config['onchain_tips_filename'])
        tips = super().get_current_document_version(pipeline_config['offchain_tips'])
        distribution_round = super().get_current_document_version("distribution_round")[0]
        users = super().get_current_document_version("users")
        distribution_allocation = super().get_current_document_version("distribution_allocation")[0]

        onchain_tips = [oc for oc in onchain_tips if (oc['timestamp'] >= distribution_round['from_date']) &
                        (oc['timestamp'] <= distribution_round['to_date'])]

        self.logger.info(f"unzip raw zip file to temp directory...")
        raw_file = super().get_current_document_version("raw_zip")[0]
        unzip_path = str(os.path.join(pipeline_config['temp_dir']))

        with zipfile.ZipFile(raw_file['zip_path'], 'r') as zip_ref:
            zip_ref.extractall(unzip_path)

        # get post metadata
        self.logger.info(f"compile post meta from raw zip files...")
        post_meta = []
        for file in glob.glob(os.path.join(unzip_path, 'posts_*')):
            with open(file, 'r', encoding="utf8") as csv_file:
                next(csv_file, None)  # skip header
                reader = csv.reader(csv_file, delimiter=',')
                for row in reader:
                    post_meta.append({
                        'id': row[0],
                        'score': int(row[1]),
                        'author': row[2],
                        'date': row[3],
                        'comments': int(row[4]),
                        'flair': row[5],
                    })

        # get pay2post metadata
        self.logger.info(f"compile pay2post meta from raw zip files...")
        pay2post = []
        for file in glob.glob(os.path.join(unzip_path, 'pay2post_*')):
            with open(file, 'r', encoding="utf8") as csv_file:
                next(csv_file, None)  # skip header
                reader = csv.reader(csv_file, delimiter=',')
                for row in reader:
                    pay2post.append({
                        'id': row[0],
                        'author': row[1],
                        'date': row[2]
                    })

        # get all comment metadata
        self.logger.info(f"compile comment meta from raw zip files...")
        comment_meta = []
        files = glob.glob(os.path.join(unzip_path, 'comments_*'))
        files.extend(glob.glob(os.path.join(unzip_path, 'daily_*')))
        for file in files:
            with open(file, 'r', encoding="utf8") as csv_file:
                next(csv_file, None)  # skip header
                reader = csv.reader(csv_file, delimiter=',')
                for row in reader:
                    comment_meta.append({
                        'id': row[0],
                        'score': int(row[1]),
                        'author': row[2],
                        'date': row[3],
                        'submission': row[4],
                        'multiplier': .5 if "daily" in file else 1,
                    })

        # map onchain tips to have the same format as offchain tips and save them in the tips array
        for oc_tip in onchain_tips:
            tips.append({
                "from_user": oc_tip["from_user"],
                "to_user": oc_tip["to_user"],
                "amount": oc_tip["amount"],
                "weight": oc_tip["weight"],
                "token": oc_tip["token"],
                "content_id": None,
                "parent_content_id": oc_tip['content_id'],
                "submission_content_id": oc_tip['content_id'][3:],
                "created_date": oc_tip["timestamp"]
            })

        c2v_posts = []
        c2v_comments = []

        itr = 0
        tip_count = len(tips)
        for tip in tips:
            itr += 1
            self.logger.info(f"processing tip [{itr}/{tip_count}] content_id: {tip['parent_content_id']}")

            if tip['parent_content_id'][:3] == 't3_':
                post = [p for p in c2v_posts if p['id'] == tip['parent_content_id']]
                if post:
                    # post previously processed and has tips associated with it
                    post = post[0]

                    # see if this user has tipped the post previously
                    previous_tips = [t for t in post['tips'] if t['from_user'].lower().strip() == tip['from_user'].lower().strip()]
                    if previous_tips:
                        # this user has previously tipped this post
                        self.logger.info("  post previously tipped by this user.")

                        # determine if the weight of this tip is greater than the weight of previous tips
                        previous_max_weight = float(max(previous_tips, key=lambda x: x['weight'])['weight'])
                        if float(tip['weight']) > previous_max_weight:
                            self.logger.info(f'  switching weight from {previous_max_weight} to {tip['weight']}')
                            post['weight'] -= previous_max_weight  # remove the weight previously associated
                            post['weight'] += float(tip['weight'])
                            post['amount'] += float(tip['amount'])

                        continue
                    else:
                        post['upvotes'] = post['upvotes'] + 1
                        post['weight'] = post['weight'] + float(tip['weight'])
                        post['amount'] += float(tip['amount'])
                        post['tips'].append(tip)

                else:
                    meta = next((m for m in post_meta if m['id'] == tip['parent_content_id'][3:]), None)

                    if not meta:
                        self.logger.info(f"  post meta for post: {tip['parent_content_id']} not found ....")
                        continue

                    flair = meta['flair']
                    if not flair:
                        multiplier = 1
                    else:
                        match flair.lower().strip():
                            case 'original content':
                                multiplier = 2
                            case 'link':
                                multiplier = .75
                            case 'comedy' | 'media' | 'self story':
                                multiplier = .25
                            case 'question':
                                 multiplier = .1
                            case _:
                                multiplier = 1

                    c2v_posts.append({
                        'id': tip['parent_content_id'],
                        'flair': flair,
                        'multiplier': multiplier,
                        'author': tip['to_user'],
                        'amount': float(tip['amount']),
                        'upvotes': 1,
                        'weight': float(tip['weight']) * multiplier,
                        'tips': [
                            tip
                        ]
                    })

            elif tip['parent_content_id'][:3] == 't1_':
                comment_record = [c for c in c2v_comments if c['id'] == tip['parent_content_id']]
                if comment_record:
                    comment_record = comment_record[0]
                    previous_tips = [t for t in comment_record['tips'] if t['from_user'].lower().strip() ==
                                     tip['from_user'].lower().strip()]
                    if previous_tips:
                        # this user has previously tipped this comment
                        self.logger.info("  comment previously tipped by this user.")

                        # determine if the weight of this tip is greater than the weight of previous tips
                        previous_max_weight = float(max(previous_tips, key=lambda x: x['weight'])['weight'])
                        if float(tip['weight']) > previous_max_weight:
                            self.logger.info(f'  switching weight from {previous_max_weight} to {tip['weight']}')
                            comment_record['weight'] -= previous_max_weight  # remove the weight previously associated
                            comment_record['weight'] += float(tip['weight'])
                            comment_record['amount'] += float(tip['amount'])

                        continue
                    else:
                        comment_record['upvotes'] = comment_record['upvotes'] + 1
                        comment_record['weight'] = comment_record['weight'] + float(tip['weight'])
                        comment_record['amount'] += float(tip['amount'])
                        comment_record['tips'].append(tip)
                else:
                    meta = next((m for m in comment_meta if m['id'] == tip['parent_content_id'][3:]), None)

                    if not meta:
                        self.logger.info(f"  comment meta for comment: {tip['parent_content_id']} not found ....")
                        continue

                    c2v_comments.append({
                        'id': tip['parent_content_id'],
                        'author': tip['to_user'],
                        'weight': float(tip['weight']) * meta['multiplier'],
                        'multiplier': meta['multiplier'],
                        'amount': float(tip['amount']),
                        'upvotes': 1,
                        'tips': [
                            tip
                        ]
                    })
            else:
                self.logger.error(f"unable to process tip for content_id={tip['content_id']}")
                continue

        post_ratio = round(int(distribution_allocation['posts']) / sum([p['weight'] for p in c2v_posts]), 5)
        comment_ratio = round(int(distribution_allocation['comments']) / sum(c['weight'] for c in c2v_comments), 5)
        p2p_ratio = round(min(post_ratio * 2.5, 250), 5)

        self.logger.info(f"  post ratio: {post_ratio:.5f}")
        self.logger.info(f"  comment ratio: {comment_ratio:.5f}")
        self.logger.info(f"  pay2post ratio: {p2p_ratio:.5f}")

        # create a base csv object that conforms to the layout of the legacy csv file received
        base_csv = []
        self.logger.info("building base .csv file...")
        for user in users:
            username = user['username'].lower()
            comments = [c for c in c2v_comments if c['author'].lower().lower().strip() == username.lower().strip()]
            posts = [p for p in c2v_posts if p['author'].lower().lower().strip() == username.lower().strip()]

            if not comments and not posts:
                continue

            comment_score = sum([c['weight'] for c in comments]) if comments else 0
            post_score = sum([p['weight'] for p in posts]) if posts else 0
            p2p_posts = len([p for p in pay2post if p['author'].lower().strip() == username.lower().strip()])
            p2p_penalty = round(p2p_ratio * p2p_posts, 5)

            base_csv.append({
                'username': username,
                'comments': len(comments),
                'comment_upvotes': round(comment_score, 5),
                'comment_score': round(comment_score * comment_ratio, 5),
                'posts': len(posts),
                'post_upvotes': round(post_score, 5) or 0,
                'post_score': round(post_score * post_ratio, 5),
                'total_posts': p2p_posts or 0,
                'pay2post': p2p_penalty or 0,
                'points': round((comment_score * comment_ratio) + (post_score * post_ratio) - p2p_penalty, 5),
                'blockchain_address': user['address']
            })

        base_csv = sorted(base_csv, key=lambda x: x['points'], reverse=True)

        # make the file available downstream to other pipeline processes
        super().save_document_version(base_csv, 'distribution')

        return super().update_pipeline(pipeline_config, {
            'distribution': 'distribution',
            'post_ratio': post_ratio,
            'comment_ratio': comment_ratio,
            'p2p_ratio': p2p_ratio
        })
