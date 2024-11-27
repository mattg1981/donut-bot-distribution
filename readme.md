# Donut-Bot Distribution

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [FAQ's](/faq.md)
- [Contributing](#contributing)
- [License](#license)

## Overview

donut-bot-distribution is designed to facilitate the distribution of Donuts ([an ERC-20 token](https://arbiscan.io/token/0xf42e2b8bc2af8b110b65be98db1321b1ab8d44f5)) and Contrib ([a souldbound ERC-20 token](https://arbiscan.io/token/0xF28831db80a616dc33A5869f6F689F54ADd5b74C)) within the r/EthTrader subreddit. The end result of this process creates: 
- a Safe(WALLET) transaction (formally Gnosis Safe) file that is imported into the multi-sig wallet.
- a legacy .json file used to update the [Donut Dashboard](https://donut-dashboard.com/#/distribution)
- a detailed log file

## Features

- **Safe Transaction Creation**: Generates transaction to be imported into the multi-sig wallet.
- **Ad-hoc Transaction Builder**: Generates a Safe transaction for one-off payments (such as monthly contests or Donut Initiatives)
- **Calculation of Tip2Vote**: Calculates the base t2v .csv file

## Installation

To get started with Donut-Bot, follow these steps:

1. Clone the repository:

```bash
git clone https://github.com/mattg1981/donut-bot-distribution.git
cd donut-bot-distribution
```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```
## Usage

Run the bot with the following command:

```bash
python main.py
```

Unless you are trying to run this process for a previous round, no configuration changes or arguments are needed to run it for the current distribution round.  The results of the process will be found in the `out\round_{distribution_round}\` directory. 

## Configuration

Configuration details can be found in the `config.json` file. 

## Contributing

Contributions are welcome! If you have any suggestions or improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

