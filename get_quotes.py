#!/usr/bin/env python2.7

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

alphavantage_last_query = None
alphavantage_min_delay = datetime.timedelta(seconds=12)

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


def alphavantage_delay():
    global alphavantage_last_query
    global alphavantage_min_delay
    
    now = datetime.datetime.now()
    
    if alphavantage_last_query is None:
        alphavantage_last_query = now
        return

    diff = now - alphavantage_last_query
    if diff < alphavantage_min_delay:
        sleep_time = alphavantage_min_delay - diff
        get_logger().debug("Sleeping for %d seconds for alphavantage", sleep_time.total_seconds())
        time.sleep(sleep_time.total_seconds())

    alphavantage_last_query = now
    

def call_gnc_fq(symbol, source_name):
    if source_name == "alphavantage":
        alphavantage_delay()

    get_logger().debug("Getting price symbol %s source %s", symbol, source_name)
        
    input_string = '({} "{}")'.format(source_name, symbol)
    get_logger().debug("Sending to process '%s'", input_string)
    process = subprocess.Popen(["gnc-fq-helper"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    (output, error) = process.communicate(input=input_string)
    output = output.rstrip()
    error = error.rstrip()
    get_logger().debug("output: '%s'", output)
    get_logger().debug("error: '%s'", error)

    if "(#f)" == output:
        # failed to find price
        return None, None
    else:
        # parse output
        match = re.match(r'^\(\("\S+" \(symbol \. "\S+"\) \(gnc:time-no-zone \. "[^"]+"\) \(last \. (?P<value>\d+\.\d+)\) \(currency \. "(?P<currency>\S+)"\)\)\)', output)
        if match:
            return match.group("value"), match.group("currency")
        else:
            get_logger().warn("No match on output '%s'", output)
            return None, None


def convert_float_to_gnumeric(value):
    f = fractions.Fraction(value)
    
    return gnucash.GncNumeric(f.numerator, f.denominator)

    
def update_price(book, commodity):
    source = commodity.get_quote_source()
    if source:
        source_name = gnucash.gnucash_core_c.gnc_quote_source_get_user_name(source)
    else:
        source_name = None
    get_logger().debug("symbol: %s name: %s quote: %s source: %s", commodity.get_nice_symbol(), commodity.get_fullname(), commodity.get_quote_flag(), source_name)
    if source_name is not None:
        value, currency = call_gnc_fq(commodity.get_nice_symbol(), source_name)
        get_logger().debug("Got value: %s currency: %s", value, currency)

        if value and currency:
            table = book.get_table()
            gnc_currency = table.lookup('ISO4217', currency)
            p = GncPrice(book)
            p.set_time(datetime.datetime.now())
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
    args = parser.parse_args(argv)
    setup_logging(default_path=args.logconfig)

    # FIXME get this from command line 
    filename = 'acdx-2quotes.gnucash'
    session = gnucash.Session(filename)
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
    
