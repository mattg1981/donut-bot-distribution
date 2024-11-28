# Donut-Bot Distribution FAQ's

## Table of Contents
- [Can I run this process?](#can-i-run-this-process)
- [When do I run this?](#when-do-i-run-this)
- [What are distribution tasks?](#what-are-distribution-tasks)
- [Are all these tasks still used?](#are-all-these-tasks-still-used)
- [What is document versioning?](#what-is-document-versioning)
- [What is document caching](#what-is-document-caching)
- [What is the distribution round checksum](#what-is-the-distribution-round-checksum)
- [Do I need to make any modifications to this code for new polls from snapshot.org](#do-i-need-to-make-any-modifications-to-this-code-for-new-polls-from-snapshotorg)
- [Does this need direct Donut-Bot database access?](#does-this-need-direct-donut-bot-database-access)
- [How do I build a one-off/ad-hoc transaction (e.g. for payment of a Donut Initiative)?](#how-do-i-build-a-one-offad-hoc-transaction-eg-for-payment-of-a-donut-initiative)
- [How do I know what the post/comment/pay2post ratio was?](#how-do-i-know-what-the-postcommentpay2post-ratio-was)
- [What do I do with these files after it has run to completion?](#what-do-i-do-with-these-files-after-it-has-run-to-completion)
- [Where/How are flair penalties handled?](#wherehow-are-flair-penalties-handled)
- [Where/How is the daily penalty handled?](#wherehow-is-the-daily-penalty-handled)
- [How are 50 comments per day handled?](#how-are-50-comments-per-day-handled)

## Can I run this process?
Yes, while this process produces files to be consumed by other services, it does not create any on-chain transactions or change any data.  You are free to run this application to familiarize yourself with it without the risk of any issues.

## When do I run this?
This process is to be run after the output of [mydonuts-csv-generator](https://github.com/EthTrader/mydonuts-csv-generator) is ready and uploaded to https://www.mydonuts.online/home/mydonuts/static/rounds/round_{distribution_round}.zip.  If this URL every changes, be sure to update `Task_00100_Pull_Files_For_Distribution.py` with the updated address.

## What are distribution tasks?
These are pipeline tasks (each task can add results to the pipeline and pass them downstream to be consumed by other tasks) that build the distribution pipeline and run in order by `priority`.  

Each task should inherit from `DistributionTask` and override `def process(self, pipeline_config)`.  Distribution tasks are executed in order by their `priority` with a lower number having a higher priority (e.g. `priority=0` runs before `priority=100`).  

Included in this repo is `Task_00000_Sample_task.py` to show how to create a baseline task (it does not run because its `priority < 0`).

## Are all these tasks still used?

No, check the `priority` of the task to see if it is still in use.  Any task with `priority < 0` is not in use anymore.

For example from `Task_00900_Flag_Banned_Users.py`:

```
class FlagBannedUsersDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = -900  # <-- THIS LINE HERE SETS A NEGATIVE PRIORITY
```


## What is document versioning?

In many tasks, you will see the following:

`val = super().get_current_document_version("key")`

and

`super().save_document_version(some_data_set, "key")`

Typically, a task will load a dataset (using `get_current_document_version`), perform some calculations (resulting in a change to the data - e.g. adding additional columns to the dataset) and then save the changed dataset (using `save_document_version`).  Each changed dataset is saved as a new version for transparency to show what the result that task had on that dataset.  There are additional methods available to get a specific version of the document in case you need to work from a very specific dataset (as opposed to the most current version).

## What is document caching

Some tasks calculate very 'expensive' datasets.  For example, see `Task_00200_Cache_Computationally_Expensive_Datasets.py`.  We do not want to recalculate these datasets on subsequent runs in the event the process fails for some reason and we need to make code changes (e.g. edge cases that were not accounted for in the code that caused the process to fail).  These datasets are prime examples of data that we cache and the data will be saved for future runs and not recalculated each time.  

In addition, this is used to develop a snapshot of data for certain datasets.  For example, there are datasets that are time dependent that factor into the distribution round checksum [e.g. a user's reddit status (banned, karma, age) is subject to change depending on when the process is run].

## What is the distribution round checksum

`Task_01400_Build_Safe_Transaction.py` generates and writes to a log file the distribution round checksum.  It looks like this in the log:

`2024-11-21 12:02:14,632 - Task_01400_Build_Safe_Transaction.py - INFO -   distribution round checksum: [f3f94e4064b22f1008554a52baac5c260556ec131a0496b0580a7f0cf35eee5b]`

This checksum is the resulting checksum of the `out\round_{distribution_round}\tx_builder\tx_{distribution_round}.json`.

This checksum is used as a validation that the file was not tampered with in anyway.  Anyone is able to download and run this process and they should all get the same checksum.  (Note: as mentioned in [What is document caching](#What is document caching) some files are time sensitive and should be cached to ensure anyone pulling down the latest repo gets the same cached files)

## Do I need to make any modifications to this code for new polls from snapshot.org

No, the process `Task_01100_Calculate_Voting_Incentives` uses graphql to query the subgraph and determine all new polls within ending within this distribution round.  Nothing is needed on your part to maintain this.

## Does this need direct Donut-Bot database access?

No, all datasets used in processing are publicly available at [donut-bot-output](https://github.com/mattg1981/donut-bot-output)

## How do I build a one-off/ad-hoc transaction (e.g. for payment of a Donut Initiative)?

Run `ad_hoc\donut_initiative_distribution_arb1.py` after updating the `award` variable.  This process automatically calculates the incentive for the organizer, but you will want to update who gets awarded that bonus.  This produces a safe transaction file (.json) in the same directory it is run from - see [What do I do with these files after it has run to completion?](#what-do-i-do-with-these-files-after-it-has-run-to-completion) for more information on how to load that file into Safe(WALLET).

## How do I know what the post/comment/pay2post ratio was?

When completed, view the log file and near the end you will see a block similar to:

```
2024-11-21 12:02:14,417 - Task_01100_Calculate_Voting_Incentives.py - INFO - original comment ratio was: 41.18745
2024-11-21 12:02:14,417 - Task_01100_Calculate_Voting_Incentives.py - INFO - comment ratio (after voting bonus) is now: 37.10186
2024-11-21 12:02:14,417 - Task_01100_Calculate_Voting_Incentives.py - INFO - original post ratio was: 42.6789
2024-11-21 12:02:14,417 - Task_01100_Calculate_Voting_Incentives.py - INFO - post ratio (after voting bonus) is now: 38.48
2024-11-21 12:02:14,417 - Task_01100_Calculate_Voting_Incentives.py - INFO - original pay2post: 106.69725
2024-11-21 12:02:14,417 - Task_01100_Calculate_Voting_Incentives.py - INFO - pay2post ratio (after voting bonus) is now: 96.2
```

You are interested in the ... (after voting bonus) ... lines.  The voting bonus is a very complicated process that changes the ratio's originally calculated in `Task_00150_Build_Comment2Vote.py`

## What do I do with these files after it has run to completion?
Once completed, `git commit` the files back to the repo.  Then, do the following:

### `out\round_{round}\legacy\distributionSummary.json`: 
Commit this to the https://github.com/EthTrader/donut.distribution repo.  This may be easiest to do this from the github website.  To do so, 
- Navigate to the [donut.distribution -> distributionSummary.json](https://github.com/EthTrader/donut.distribution/blob/main/docs/distributionSummary.json) file 
- click the pencil icon (Edit this file) and then paste the contents of this file 
- click the 'Commit changes...' button.  
- Once this has been committed, the Donut Dashboard will pick up the changes.

### `out\round_{round}\tx_builder\arb1_tx_{round}.json`: 
- Navigate to the [Arb One Safe Wallet Multi-Sig](https://app.safe.global/home?safe=arb1:0x439ceE4cC4EcBD75DC08D9a17E92bDdCc11CDb8C)
- Connect your wallet (you must be a multi-sig guardian)
- Click 'New Transaction'
- Click 'Transaction Builder'
- Drag and drop the file to 'Drag and drop a JSON file or choose a file'
- Follow the rest of the steps to add the transaction

## Where/How are flair penalties handled?
Flair penalties are handled in `Task_00150_Build_Comment2Vote.py` and the multiplier is stored in a variable called `multiplier`.  Look for this block of code (or a similar block if there have been modifications):

```
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
```

## Where/How is the daily penalty handled?
The daily penalty is handled in `Task_00150_Build_Comment2Vote.py` and the multiplier is stored in a variable called `multiplier`.  Look for this block of code (or a similar block if there have been modifications):

```
files = glob.glob(os.path.join(unzip_path, 'comments_*_limited*'))
files.extend(glob.glob(os.path.join(unzip_path, 'daily_*_limited*')))
for file in files:
    with open(file, 'r', encoding="utf8") as csv_file:
        next(csv_file, None)  # skip header
        reader = csv.reader(csv_file, delimiter=',')
        for row in reader:
            comment_meta.append({
                'id': row[1],
                'score': row[2],
                'author': row[3],
                'date': row[4],
                'submission': row[5],
                'multiplier': .5 if "daily" in file else 1,
            })

```

## How are 50 comments per day handled?
The logic for 50 comments per day is handled by [mydonuts-csv-generator](https://github.com/EthTrader/mydonuts-csv-generator), which filters the .csv files to only the first 50 comments per day per user.  

When `Task_00150_Build_Comment2Vote.py` is iterating all the tips, it checks to see if the `tip['parent_content_id']` exists in any of those files.  If that comment does exist, the tip is registered as an upvote.  If that comment does not exist in the file, the tip is ignored.