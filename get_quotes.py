#!/usr/bin/env python

import warnings
with warnings.catch_warnings():
    import re
    import sys
    import argparse
    import os
    import os.path
    import logging
    import gnucash 
    import logging
    import logging.config
    import json
    import subprocess
    import datetime
    import time
    from gnucash_patch import GncPrice
    import fractions
    
script_dir=os.path.abspath(os.path.dirname(__file__))

# Maximum number of times to retry getting a quote.
# This only applies to sources that have a delay listed.
# Each time the retry is done, the delay multiplier is increased.
MAX_RETRIES = 10

# quote source -> time
last_query = dict()

# minimum delay per quote source
# 5 API requests per minute is the limit to alphavantage
# That is once every 12 seconds.
delay = { 'alphavantage': datetime.timedelta(seconds=12) }


def get_logger():
    return logging.getLogger(__name__)


def setup_logging(
    default_path='logging.json',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """
    Setup logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'r') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def determine_commodities_to_check(account):
    commodities_to_check = set()
    for acc in account.get_descendants():
        if acc.GetType() == gnucash.ACCT_TYPE_STOCK or acc.GetType() == gnucash.ACCT_TYPE_MUTUAL:
            if acc.GetBalance().to_double() > 0:
                commodity = acc.GetCommodity()
                namespace = commodity.get_namespace()
                if namespace != 'CURRENCY':
                    commodities_to_check.add(commodity)

    return commodities_to_check


def call_gnc_fq(symbol, source_name):
    get_logger().debug("Getting price symbol %s source %s", symbol, source_name)
        
    input_string = '({} "{}")'.format(source_name, symbol)
    get_logger().debug("Sending to process '%s'", input_string)
    process = subprocess.Popen(["gnc-fq-helper"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    (output, error) = process.communicate(input=input_string)
    output = output.rstrip()
    error = error.rstrip()
    get_logger().debug("output: '%s'", output)
    get_logger().debug("error: '%s'", error)

    if re.match(r'^(#f)', output) is not None:
        # failed to find price
        return None, None, None
    else:
        # parse output
        match = re.match(r'^\(\("\S+" \(symbol \. "\S+"\) \(gnc:time-no-zone \. "(?P<datetime>[^"]+)"\) \(last \. (?P<value>\d+\.\d+)\) \(currency \. "(?P<currency>\S+)"\)\)\)', output)
        if match:
            return match.group("value"), match.group("currency"), match.group("datetime")
        else:
            get_logger().warn("No match on output '%s'", output)
            return None, None, None

        
def execute_delay(source_name, quote_delay, multiplier):
    global last_query

    quote_last_query = last_query.get(source_name, None)
    
    now = datetime.datetime.now()
    
    if quote_last_query is not None:
        diff = now - quote_last_query
        wait_time = quote_delay * multiplier
        if diff < wait_time:
            sleep_time = wait_time - diff
            get_logger().debug("Sleeping for %d seconds for %s", sleep_time.total_seconds(), source_name)
            time.sleep(sleep_time.total_seconds())

    # use time after quote is finished to ensure that we don't creep up on the API limit
    last_query[source_name] = datetime.datetime.now()

    
def get_quote(symbol, source_name):
    global delay
    global last_query
    global MAX_RETRIES
    
    attempt = 1
    quote_delay = delay.get(source_name, None)

    # <= so that we try at least MAX_RETRIES times since we start at 1
    while attempt <= MAX_RETRIES:
        if quote_delay is not None:
            execute_delay(source_name, quote_delay, attempt)

        value, currency, quote_datetime = call_gnc_fq(symbol, source_name)
        if quote_delay is not None and value is None:
            # if we failed to get a quote and there is a delay for this source, try again
            attempt = attempt + 1
            continue
        else:
            return value, currency, quote_datetime

    get_logger().debug("Exhausted retries for %s with %s", symbol, source_name)
    return None, None, None
    
        
        
def convert_float_to_gnumeric(value):
    f = fractions.Fraction(value)
    
    return gnucash.GncNumeric(f.numerator, f.denominator)


def parse_datetime(s):
    if s is None:
        return datetime.datetime.now()

    # 2019-11-29 12:00:00
    dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return dt


def update_price(book, commodity):
    source = commodity.get_quote_source()
    if source:
        source_name = gnucash.gnucash_core_c.gnc_quote_source_get_user_name(source)
    else:
        source_name = None
    get_logger().debug("symbol: %s name: %s quote: %s source: %s", commodity.get_nice_symbol(), commodity.get_fullname(), commodity.get_quote_flag(), source_name)
    if source_name is not None:
        value, currency, quote_datetime = get_quote(commodity.get_nice_symbol(), source_name)
        get_logger().debug("Got value: %s currency: %s datetime: %s", value, currency, quote_datetime)
        
        if value and currency:
            table = book.get_table()
            gnc_currency = table.lookup('ISO4217', currency)
            p = GncPrice(book)
            p.set_time(parse_datetime(quote_datetime))
            p.set_commodity(commodity)
            p.set_currency(gnc_currency)
            gnumeric_value = convert_float_to_gnumeric(value)
            p.set_value(gnumeric_value)
            p.set_source(gnucash.gnucash_core_c.PRICE_SOURCE_FQ)
            book.get_price_db().add_price(p)
    

def update_prices(book, commodities_to_check):
    for commodity in commodities_to_check:
        update_price(book, commodity)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--logconfig", dest="logconfig", help="logging configuration (default: logging.json)", default='logging.json')
    parser.add_argument("-f", "--file", dest="filename", help="file to read (required)", required=True)
    args = parser.parse_args(argv)
    setup_logging(default_path=args.logconfig)

    if not os.path.exists(args.filename):
        get_logger().error("%s doesn't exist", args.filename)
        return 1

    lockfile = args.filename + ".LCK"
    if os.path.exists(lockfile):
        get_logger().error("Lockfile exists, cannot proceed")
        return 1
    
    session = gnucash.Session(args.filename)
    try:
        book = session.book
        table = book.get_table()
        pricedb = book.get_price_db()
        currency_code = 'USD'
        currency = table.lookup('ISO4217', currency_code)
        account = book.get_root_account()

        commodities_to_check = determine_commodities_to_check(account)

        update_prices(book, commodities_to_check)
        
        session.save()
    finally:
        session.end()
        session.destroy()
        
if __name__ == "__main__":
    sys.exit(main())
    
