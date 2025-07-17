#!/usr/bin/env python3

import warnings
with warnings.catch_warnings():
    import re
    import sys
    import argparse
    import os
    import logging
    import gnucash 
    import logging
    import logging.config
    import json
    from pathlib import Path
    import subprocess
    import datetime
    import time
    from gnucash_patch import GncPrice
    import fractions
    import platformdirs
    
SCRIPT_DIR=Path(__file__).parent.absolute()

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
    try:
        path = Path(default_path)
        value = os.getenv(env_key, None)
        if value:
            path = Path(value)
        if path.exists():
            with open(path, 'r') as f:
                config = json.load(f)
            logging.config.dictConfig(config)
        else:
            logging.basicConfig(level=default_level)
    except:
        print(f"Error configuring logging, using default configuration with level {default_level}: {err=}, {type(err)=}")
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
    process = subprocess.Popen(["gnc-fq-helper"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
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
        match = re.match(r'^\(\("\S+" \(symbol \. "\S+"\) \(gnc:time-no-zone \. "(?P<datetime>[^"]+)"\) \(last \. (#e)?(?P<value>\d+\.\d+)\) \(currency \. "(?P<currency>\S+)"\)\)\)', output)
        if match:
            return match.group("value"), match.group("currency"), match.group("datetime")
        else:
            get_logger().warning("No match on output '%s'", output)
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


def get_current_price(symbol, source_name):
    """
    Returns:
      value: str
      currency: str
      quote date: str
    """
    get_logger().debug("Getting price symbol %s source %s", symbol, source_name)
    
    # FIXME allow one to specify flatpak or direct
    gnucash_command = "flatpak run --command=gnucash-cli org.gnucash.GnuCash"
    #gnucash_command = "gnucash-cli"
    
    output = subprocess.check_output(f"{gnucash_command} --quotes dump {source_name} {symbol}", shell=True)
    output = output.decode().rstrip()
    get_logger().debug("output: '%s'", output)


    date = None
    value = None
    currency = None
    for line in output.splitlines():
        if m := re.match(r'^\s*date:\s+(\S+)', line):
            date = m.group(1)
        elif m := re.match(r'^\s*currency:\s+(\S+)', line):
            currency = m.group(1)
        elif m := re.match(r'^\s*(?:last|nav|price):\s+(\S+)', line):
            value = m.group(1)

        if date and value and currency:
            return value, currency, date
        
    # failed to find price
    import pdb
    pdb.set_trace()
    return None, None, None
    
    
def get_quote(symbol, source_name):
    global delay
    global last_query
    global MAX_RETRIES

    get_logger().debug("Getting quote for %s in %s", symbol, source_name)
    
    attempt = 1
    quote_delay = delay.get(source_name, None)

    # <= so that we try at least MAX_RETRIES times since we start at 1
    while attempt <= MAX_RETRIES:
        if quote_delay is not None and attempt > 1:
            get_logger().debug("Executing delay attempt: %s", attempt)
            execute_delay(source_name, quote_delay, attempt)

        #value, currency, quote_datetime = call_gnc_fq(symbol, source_name)
        value, currency, quote_datetime = get_current_price(symbol, source_name)
        if quote_delay is not None and value is None:
            # if we failed to get a quote and there is a delay for this source, try again
            attempt = attempt + 1
            continue
        else:
            get_logger().debug("Returning value: %s", value)
            return value, currency, quote_datetime

    get_logger().debug("Exhausted retries for %s with %s", symbol, source_name)
    return None, None, None
        
        
def convert_float_to_gnumeric(value):
    f = fractions.Fraction(value)
    
    return gnucash.GncNumeric(f.numerator, f.denominator)


def parse_datetime(s):
    if s is None:
        return datetime.datetime.now()

    # 2019-11-29
    dt = datetime.datetime.strptime(s, "%m/%d/%Y")
    return dt


def update_price(book, commodity):
    """
    Returns:
      bool: success
    """
    source = commodity.get_quote_source()
    if source:
        source_name = gnucash.gnucash_core_c.gnc_quote_source_get_internal_name(source)
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
            p.set_time64(parse_datetime(quote_datetime))
            p.set_commodity(commodity)
            p.set_currency(gnc_currency)
            gnumeric_value = convert_float_to_gnumeric(value)
            p.set_value(gnumeric_value)
            p.set_source(gnucash.gnucash_core_c.PRICE_SOURCE_FQ)
            book.get_price_db().add_price(p)
            return True
    return False


def get_save_file() -> Path:
    """
    Returns
      path to where the data is saved, file may not exist
    """
    data_dir = Path(platformdirs.user_data_dir(appname='gnucash-quotes', appauthor='jpschewe', ensure_exists=True))
    get_logger().debug("Data dir %s", data_dir)
    
    return data_dir / 'state.json'


def save_state(source_name: str, commodity_symbol: str):
    save_data = dict()
    save_data['source'] = source_name
    save_data['commodity'] = commodity_symbol
    
    with open(get_save_file(), 'w') as f:
        json.dump(save_data, f)


def get_state() -> tuple[str|None, str|None]:
    save_file = get_save_file()
    if not save_file.exists():
        return None, None
    with open(save_file, "r") as f:
        save_data = json.load(f)
        return save_data.get('source'), save_data.get('commodity')


def get_source_name(commodity) -> str|None:
    source = commodity.get_quote_source()
    if source:
        return gnucash.gnucash_core_c.gnc_quote_source_get_internal_name(source)
    else:
        return None
        

def update_prices(book, commodities_to_check):
    """
    Returns:
      bool: True if some prices were retrieved or there were no commodities to check
    """
    sorted_commodities = list(sorted(commodities_to_check, key=lambda c: c.get_nice_symbol()))
    get_logger().debug("Sorted commodities: %d %s", len(sorted_commodities), (c.get_nice_symbol() for c in sorted_commodities))

    last_source_name, last_commodity_symbol = get_state()
    get_logger().debug("Found saved state %s %s", last_source_name, last_commodity_symbol)

    skip=True
    # the last commodity successfully checked
    prev_commodity = None
    for commodity in sorted_commodities:
        commodity_symbol = commodity.get_nice_symbol()

        source_name = get_source_name(commodity)
        if source_name is None:
            get_logger().debug("Skipping %s with no source", commodity_symbol)
            continue
            
        if skip:
            if last_source_name is None:
                skip = False
            elif last_source_name == source_name and last_commodity_symbol == commodity_symbol:
                skip = False
                # skip this commodity and continue with the next
                continue
            else:
                # keep skipping
                continue
            
        if not update_price(book, commodity):
            break

        prev_commodity = commodity
        
        get_logger().info("STOP for DEBUG")
        break


    if prev_commodity is not None:
        prev_source_name = get_source_name(prev_commodity)
        prev_commodity_symbol = prev_commodity.get_nice_symbol()
        
        save_state(prev_source_name, prev_commodity_symbol)
    else:
        get_logger().error("Unable to get any quotes")
        if skip:
            get_logger().info("FIXME needs to go back to the top of the loop with skip = False")
            skip = False
        return False

    global last_query
    get_logger().info("dump of last query %s", last_query)
    
    return True
        

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

        result = update_prices(book, commodities_to_check)
        
        session.save()
    finally:
        session.end()
        session.destroy()

    if result:
        return 0
    else:
        return 1
        
if __name__ == "__main__":
    sys.exit(main())
    
